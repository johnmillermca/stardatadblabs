package io.stardata.plugin;

import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginContext;
import org.apache.doris.plugin.PluginException;
import org.apache.doris.plugin.PluginInfo;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DdlLoadTrackerPlugin
 *
 * Doris Audit Plugin. Fires asynchronously on every completed CREATE TABLE
 * and ALTER TABLE — zero impact on the query execution thread.
 *
 * Detection: parses the DDL text from the audit event to find:
 *   (a) A column defined as  DATETIME DEFAULT CURRENT_TIMESTAMP  (no ON UPDATE)
 *   (b) A column defined as  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
 *
 * When both are found, creates an empty tracking table under cache_system:
 *
 *   CREATE TABLE IF NOT EXISTS cache_system.<table_name> (
 *       database_name  VARCHAR(256),
 *       table_name     VARCHAR(256),
 *       <created_col>  DATETIME,
 *       <updated_col>  DATETIME,
 *       snap_id        BIGINT,
 *       status         VARCHAR(32)
 *   ) UNIQUE KEY(database_name, table_name) ...
 *
 * No data is inserted by the plugin. The application populates values separately.
 *
 * Config keys in plugin.properties (env vars override):
 *   doris.host  / STARDATA_HOST   (default: 127.0.0.1)
 *   doris.port  / STARDATA_PORT   (default: 9030)
 *   doris.user  / STARDATA_USER   (default: root)
 *   doris.pass  / STARDATA_PASS   (default: "")
 */
public class DdlLoadTrackerPlugin extends Plugin implements AuditPlugin {

    private static final Logger LOG =
        Logger.getLogger(DdlLoadTrackerPlugin.class.getName());

    // StmtType values emitted by Doris in the audit log (verified from fe.audit.log)
    private static final String STMT_CREATE = "CREATE";
    private static final String STMT_ALTER  = "ALTER";

    private static final Set<String> SKIP_DBS = Set.of(
        "information_schema", "mysql", "performance_schema",
        "cache_system", "__internal_schema", "doris_audit_db__"
    );

    // Extract db.table from CREATE TABLE [IF NOT EXISTS] `db`.`tbl`
    private static final Pattern RE_CREATE = Pattern.compile(
        "(?i)\\bCREATE\\s+(?:EXTERNAL\\s+)?TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?" +
        "`?([\\w$]+)`?\\.`?([\\w$]+)`?"
    );
    // Extract db.table from ALTER TABLE `db`.`tbl`
    private static final Pattern RE_ALTER = Pattern.compile(
        "(?i)\\bALTER\\s+TABLE\\s+`?([\\w$]+)`?\\.`?([\\w$]+)`?"
    );

    /*
     * Column detection — applied to the full DDL text.
     *
     * RE_COL_UPDATED  matches:  `col` ... DATETIME ... DEFAULT CURRENT_TIMESTAMP ... ON UPDATE CURRENT_TIMESTAMP
     * RE_COL_CREATED  matches:  `col` ... DATETIME ... DEFAULT CURRENT_TIMESTAMP   (no ON UPDATE after it)
     *
     * Both patterns capture group(1) = column name.
     * Doris does NOT expose ON UPDATE in information_schema.columns.EXTRA —
     * parsing the raw DDL text is the only reliable detection approach.
     */
    private static final Pattern RE_COL_UPDATED = Pattern.compile(
        "(?i)`([\\w$]+)`[^,)]*?DATETIME[^,)]*?DEFAULT\\s+CURRENT_TIMESTAMP[^,)]*?" +
        "ON\\s+UPDATE\\s+CURRENT_TIMESTAMP"
    );
    private static final Pattern RE_COL_CREATED = Pattern.compile(
        "(?i)`([\\w$]+)`[^,)]*?DATETIME[^,)]*?DEFAULT\\s+CURRENT_TIMESTAMP" +
        "(?![^,)]*?ON\\s+UPDATE)"
    );

    private String jdbcUrl;
    private String dbUser;
    private String dbPass;

    // MySQL JDBC driver instance — bypasses DriverManager classloader isolation
    private java.sql.Driver jdbcDriver;

    // Persistent connection held by the single worker thread
    private volatile Connection sharedConn;

    // Unbounded queue — exec() must NEVER block; DDL events are extremely rare
    // Each entry: [db, table, created_col_name, updated_col_name]
    private final LinkedBlockingQueue<String[]> taskQueue = new LinkedBlockingQueue<>();
    private Thread workerThread;
    private volatile boolean running = true;

    // =========================================================================
    // Plugin lifecycle
    // =========================================================================

    @Override
    public void init(PluginInfo info, PluginContext ctx) throws PluginException {
        // Instantiate MySQL driver directly — DriverManager.getConnection() fails in
        // plugin classloaders because the driver registers itself under a child CL
        // that DriverManager (system CL) cannot see.
        try {
            jdbcDriver = (java.sql.Driver)
                Class.forName("com.mysql.cj.jdbc.Driver")
                     .getDeclaredConstructor()
                     .newInstance();
            LOG.info("[ddl-load-tracker] loaded driver: com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            throw new PluginException(
                "MySQL JDBC driver not found (com.mysql.cj.jdbc.Driver): " + e.getMessage());
        }

        String host = env("STARDATA_HOST",
            info.getProperties().getOrDefault("doris.host", "127.0.0.1"));
        String port = env("STARDATA_PORT",
            info.getProperties().getOrDefault("doris.port", "9030"));
        dbUser = env("STARDATA_USER",
            info.getProperties().getOrDefault("doris.user", "root"));
        dbPass = env("STARDATA_PASS",
            info.getProperties().getOrDefault("doris.pass", ""));

        jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/cache_system"
                + "?useSSL=false&allowPublicKeyRetrieval=true"
                + "&connectTimeout=5000&socketTimeout=30000"
                + "&characterEncoding=utf8";

        workerThread = new Thread(this::workerLoop, "ddl-load-tracker-worker");
        workerThread.setDaemon(true);
        workerThread.start();

        LOG.info("[ddl-load-tracker] init OK -> " + jdbcUrl + " user=" + dbUser);
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (workerThread != null) workerThread.interrupt();
        closeConn();
    }

    // =========================================================================
    // AuditPlugin
    // =========================================================================

    @Override
    public boolean eventFilter(AuditEvent.EventType type) {
        return type == AuditEvent.EventType.AFTER_QUERY;
    }

    @Override
    public void exec(AuditEvent event) {
        String stmtType = event.stmtType;
        if (stmtType == null) return;
        if (!stmtType.equals(STMT_CREATE) && !stmtType.equals(STMT_ALTER)) return;

        String stmt = event.stmt;
        if (stmt == null) return;

        // Extract db + table from DDL text (event.db is empty in this Doris build)
        String[] dbTbl = extractDbTable(stmt, stmtType);
        if (dbTbl == null) return;
        if (SKIP_DBS.contains(dbTbl[0].toLowerCase())) return;

        // Detect qualifying columns directly from the DDL text.
        // ON UPDATE is NOT visible in information_schema.columns.EXTRA in Doris,
        // so raw DDL parsing is the only reliable approach.
        String updCol = firstMatch(RE_COL_UPDATED, stmt);
        if (updCol == null) return;   // no ON UPDATE CURRENT_TIMESTAMP column → skip

        String creCol = firstMatch(RE_COL_CREATED, stmt);
        if (creCol == null) return;   // no plain DATETIME DEFAULT CURRENT_TIMESTAMP column → skip

        // Queue: [db, table, created_col, updated_col] — exec() returns immediately
        taskQueue.offer(new String[]{ dbTbl[0], dbTbl[1], creCol, updCol });
    }

    // =========================================================================
    // Worker loop — single persistent thread, reconnects on failure
    // =========================================================================

    private void workerLoop() {
        LOG.info("[ddl-load-tracker] worker started");
        while (running) {
            try {
                String[] task = taskQueue.poll(60, TimeUnit.SECONDS);
                if (task == null) continue;
                createCacheTable(task[0], task[1], task[2], task[3]);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOG.log(Level.WARNING, "[ddl-load-tracker] unexpected worker error", e);
            }
        }
        LOG.info("[ddl-load-tracker] worker stopped");
    }

    /**
     * Creates an empty tracking table under cache_system named after the app table.
     * Uses IF NOT EXISTS — safe to re-run on ALTER TABLE events.
     *
     *   cache_system.<tbl> (
     *       database_name  VARCHAR(256),
     *       table_name     VARCHAR(256),
     *       <creCol>       DATETIME,
     *       <updCol>       DATETIME,
     *       snap_id        BIGINT,
     *       status         VARCHAR(32)
     *   )
     */
    private void createCacheTable(String db, String tbl,
                                  String creCol, String updCol) {
        // Build DDL — column names come from the app table's own DDL, backtick-quoted
        String ddl =
            "CREATE TABLE IF NOT EXISTS `cache_system`.`" + tbl + "` (\n" +
            "    `database_name`  VARCHAR(256)  NULL,\n" +
            "    `table_name`     VARCHAR(256)  NULL,\n" +
            "    `" + creCol + "`  DATETIME      NULL,\n" +
            "    `" + updCol + "`  DATETIME      NULL,\n" +
            "    `snap_id`        BIGINT        NULL,\n" +
            "    `status`         VARCHAR(32)   NULL\n" +
            ")\n" +
            "UNIQUE KEY(`database_name`, `table_name`)\n" +
            "DISTRIBUTED BY HASH(`database_name`) BUCKETS 4\n" +
            "PROPERTIES (\n" +
            "    \"replication_num\" = \"1\",\n" +
            "    \"enable_unique_key_merge_on_write\" = \"true\"\n" +
            ")";

        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                Connection conn = getConn();
                try (Statement st = conn.createStatement()) {
                    st.executeUpdate(ddl);
                }
                LOG.info(String.format(
                    "[ddl-load-tracker] CREATED cache_system.%s  " +
                    "(source: %s.%s  cre=%s  upd=%s)",
                    tbl, db, tbl, creCol, updCol));
                return;
            } catch (SQLException e) {
                LOG.warning("[ddl-load-tracker] attempt " + attempt +
                    " failed for cache_system." + tbl + ": " + e.getMessage());
                closeConn();
                if (attempt < 3) {
                    try { Thread.sleep(1000L * attempt); }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt(); return;
                    }
                }
            }
        }
        LOG.warning("[ddl-load-tracker] gave up creating cache_system." + tbl +
            " after 3 attempts");
    }

    // =========================================================================
    // Persistent connection management
    // =========================================================================

    private Connection getConn() throws SQLException {
        if (sharedConn == null || sharedConn.isClosed()) {
            Properties p = new Properties();
            p.setProperty("user",     dbUser);
            p.setProperty("password", dbPass);
            sharedConn = jdbcDriver.connect(jdbcUrl, p);
            if (sharedConn == null)
                throw new SQLException("Driver returned null for URL: " + jdbcUrl);
            LOG.info("[ddl-load-tracker] JDBC connection established");
        }
        return sharedConn;
    }

    private void closeConn() {
        try {
            if (sharedConn != null && !sharedConn.isClosed()) sharedConn.close();
        } catch (SQLException ignored) {}
        sharedConn = null;
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static String firstMatch(Pattern p, String text) {
        Matcher m = p.matcher(text);
        return m.find() ? m.group(1) : null;
    }

    private static String[] extractDbTable(String stmt, String stmtType) {
        Matcher m = (STMT_CREATE.equals(stmtType) ? RE_CREATE : RE_ALTER).matcher(stmt);
        return m.find() ? new String[]{ m.group(1), m.group(2) } : null;
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? v : def;
    }
}
