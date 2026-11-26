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
 * Detection: fires only for iceberg-type tables — tables whose DDL text contains
 *   PROPERTIES ("stardata.table_type" = "iceberg")
 *
 * The audit log Stmt= field contains the ORIGINAL user DDL (before the FE patch
 * injects snap_id/created_at/updated_at), so detection must be on the PROPERTIES
 * value that the user wrote, not on the injected columns.
 *
 * When an iceberg CREATE TABLE is detected, creates three tables under cache_system
 * — all with IF NOT EXISTS so re-runs on ALTER TABLE are safe:
 *
 * 1. cache_system.<tbl>          — batch control ledger
 *    Tracks every UPSERT and DELETE batch run by the sync job.
 *    Two independent cursors:
 *      snap_id_from / snap_id_to    — UPSERT window (new/changed rows, by insert order)
 *      delete_synced_until          — DELETE watermark (max deleted_at already synced)
 *
 * 2. cache_system.<tbl>_deletes  — append-only delete log
 *    The APPLICATION writes one row here on every DELETE from the main table.
 *    Schema: (snap_id BIGINT, deleted_at DATETIMEV2).
 *    Partitioned by deleted_at day — sync job reads only the new partitions:
 *      SELECT snap_id FROM cache_system.<tbl>_deletes
 *      WHERE deleted_at > :last_watermark AND deleted_at <= :new_watermark
 *    This avoids a full scan of the app table for __DORIS_DELETE_SIGN__=1 rows
 *    (which touches all daily partitions regardless of when rows were deleted).
 *    DUPLICATE KEY — not MOW — because the same snap_id can be re-used after
 *    a TRUNCATE/reload cycle.
 *
 * 3. cache_system.<tbl>_log      — error isolation log
 *    Sync job writes one row per failure. Any row blocks sync for that table
 *    until cleared by an operator (TRUNCATE TABLE from the console).
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

    // StmtType values emitted by Doris in the audit log
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
     * Detects an iceberg-type table by the stardata.table_type=iceberg property.
     * The audit log Stmt= contains the original user DDL (before the FE patch injects
     * snap_id/created_at/updated_at), so we match on the PROPERTIES value the user wrote.
     * Both single-quote and double-quote variants are matched.
     */
    private static final Pattern RE_ICEBERG = Pattern.compile(
        "(?i)[\"']stardata\\.table_type[\"']\\s*=\\s*[\"']iceberg[\"']"
    );

    private String jdbcUrl;
    private String dbUser;
    private String dbPass;

    // MySQL JDBC driver instance — bypasses DriverManager classloader isolation
    private java.sql.Driver jdbcDriver;

    // Persistent connection held by the single worker thread
    private volatile Connection sharedConn;

    // Unbounded queue — exec() must NEVER block; DDL events are extremely rare
    // Each entry: [db, table]
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

        // Only iceberg-type tables get cache_system mirror tables.
        // The audit log Stmt= contains the original user DDL — match on the
        // stardata.table_type=iceberg PROPERTIES value.
        if (!RE_ICEBERG.matcher(stmt).find()) return;

        // Extract db + table from DDL text (event.db is empty in this Doris build)
        String[] dbTbl = extractDbTable(stmt, stmtType);
        if (dbTbl == null) return;
        if (SKIP_DBS.contains(dbTbl[0].toLowerCase())) return;

        // Queue: [db, table] — exec() returns immediately, never blocks
        taskQueue.offer(new String[]{ dbTbl[0], dbTbl[1] });
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
                createIcebergMirrorTables(task[0], task[1]);
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
     * Creates three cache_system tables for every iceberg-type app table.
     * All use IF NOT EXISTS — safe to re-run on ALTER TABLE events.
     *
     * <pre>
     * Three-table pattern:
     *
     *  1. cache_system.<tbl>          — batch control ledger (13 cols)
     *     Tracks UPSERT and DELETE batches. Two independent cursors:
     *       snap_id_from/to         — UPSERT window
     *       delete_synced_until     — DELETE watermark (max deleted_at synced)
     *
     *  2. cache_system.<tbl>_deletes  — append-only delete log
     *     App writes (snap_id, deleted_at) on every DELETE from the main table.
     *     Partitioned by deleted_at day — sync job reads only new partitions.
     *     Avoids full app-table scan for __DORIS_DELETE_SIGN__=1 rows.
     *
     *  3. cache_system.<tbl>_log      — error isolation log
     *     Any row blocks sync until cleared by operator (TRUNCATE TABLE).
     * </pre>
     */
    private void createIcebergMirrorTables(String db, String tbl) {
        // ── 1. Batch control ledger ───────────────────────────────────────────
        String ddlMain =
            "CREATE TABLE IF NOT EXISTS `cache_system`.`" + tbl + "` (\n" +
            "  `batch_id`            BIGINT        NOT NULL AUTO_INCREMENT,\n" +
            "  `database_name`       VARCHAR(256)  NOT NULL,\n" +
            "  `table_name`          VARCHAR(256)  NOT NULL,\n" +
            "  `batch_type`          VARCHAR(16)   NULL,\n" +
            "  `snap_id_from`        BIGINT        NULL,\n" +
            "  `snap_id_to`          BIGINT        NULL,\n" +
            "  `delete_synced_until` DATETIMEV2    NULL,\n" +
            "  `max_created_at`      DATETIMEV2    NULL,\n" +
            "  `max_updated_at`      DATETIMEV2    NULL,\n" +
            "  `row_count`           BIGINT        NULL,\n" +
            "  `status`              VARCHAR(32)   NULL,\n" +
            "  `started_at`          DATETIMEV2    NULL,\n" +
            "  `finished_at`         DATETIMEV2    NULL\n" +
            ") UNIQUE KEY(`batch_id`)\n" +
            "DISTRIBUTED BY HASH(`batch_id`) BUCKETS 4\n" +
            "PROPERTIES (\n" +
            "  \"replication_num\" = \"1\",\n" +
            "  \"enable_unique_key_merge_on_write\" = \"true\"\n" +
            ")";

        // ── 2. Append-only delete log ─────────────────────────────────────────
        // Partitioned by deleted_at day — sync job reads only new partitions:
        //   SELECT snap_id FROM cache_system.<tbl>_deletes
        //   WHERE deleted_at > :last_watermark AND deleted_at <= :new_watermark
        // DUPLICATE KEY (not MOW) — snap_id can be reused after a truncate/reload.
        String ddlDeletes =
            "CREATE TABLE IF NOT EXISTS `cache_system`.`" + tbl + "_deletes` (\n" +
            "  `snap_id`    BIGINT     NOT NULL,\n" +
            "  `deleted_at` DATETIMEV2 NOT NULL DEFAULT CURRENT_TIMESTAMP\n" +
            ") DUPLICATE KEY(`snap_id`)\n" +
            "AUTO PARTITION BY RANGE (date_trunc('day', `deleted_at`)) ()\n" +
            "DISTRIBUTED BY HASH(`snap_id`) BUCKETS AUTO\n" +
            "PROPERTIES (\"replication_num\" = \"1\")";

        // ── 3. Error isolation log ────────────────────────────────────────────
        String ddlLog =
            "CREATE TABLE IF NOT EXISTS `cache_system`.`" + tbl + "_log` (\n" +
            "  `log_id`           BIGINT        NOT NULL AUTO_INCREMENT,\n" +
            "  `batch_id`         BIGINT        NULL,\n" +
            "  `database_name`    VARCHAR(256)  NULL,\n" +
            "  `table_name`       VARCHAR(256)  NULL,\n" +
            "  `operation`        VARCHAR(16)   NULL,\n" +
            "  `snap_id_from`     BIGINT        NULL,\n" +
            "  `snap_id_to`       BIGINT        NULL,\n" +
            "  `delete_from_ts`   DATETIMEV2    NULL,\n" +
            "  `delete_to_ts`     DATETIMEV2    NULL,\n" +
            "  `error_code`       VARCHAR(32)   NULL,\n" +
            "  `error_message`    TEXT          NULL,\n" +
            "  `failed_statement` TEXT          NULL,\n" +
            "  `rows_attempted`   BIGINT        NULL,\n" +
            "  `logged_at`        DATETIMEV2    NULL\n" +
            ") UNIQUE KEY(`log_id`)\n" +
            "DISTRIBUTED BY HASH(`log_id`) BUCKETS 4\n" +
            "PROPERTIES (\n" +
            "  \"replication_num\" = \"1\",\n" +
            "  \"enable_unique_key_merge_on_write\" = \"true\"\n" +
            ")";

        boolean mainOk = execDdl(tbl, ddlMain,
            String.format("[ddl-load-tracker] CREATED cache_system.%s  batch-tracking (source: %s.%s)",
                tbl, db, tbl));
        if (mainOk) {
            execDdl(tbl + "_deletes", ddlDeletes,
                String.format("[ddl-load-tracker] CREATED cache_system.%s_deletes  delete-log (source: %s.%s)",
                    tbl, db, tbl));
            execDdl(tbl + "_log", ddlLog,
                String.format("[ddl-load-tracker] CREATED cache_system.%s_log  error-log (source: %s.%s)",
                    tbl, db, tbl));
        }
    }

    /**
     * Execute a single DDL statement with up to 3 retries.
     * Returns true on success, false after all retries are exhausted.
     */
    private boolean execDdl(String label, String ddl, String successMsg) {
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                Connection conn = getConn();
                try (Statement st = conn.createStatement()) {
                    st.executeUpdate(ddl);
                }
                LOG.info(successMsg);
                return true;
            } catch (SQLException e) {
                LOG.warning("[ddl-load-tracker] attempt " + attempt +
                    " failed for cache_system." + label + ": " + e.getMessage());
                closeConn();
                if (attempt < 3) {
                    try { Thread.sleep(1000L * attempt); }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            }
        }
        LOG.warning("[ddl-load-tracker] gave up on cache_system." + label + " after 3 attempts");
        return false;
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

    private static String[] extractDbTable(String stmt, String stmtType) {
        Matcher m = (STMT_CREATE.equals(stmtType) ? RE_CREATE : RE_ALTER).matcher(stmt);
        return m.find() ? new String[]{ m.group(1), m.group(2) } : null;
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? v : def;
    }
}
