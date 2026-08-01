# =============================================================================
# Strimzi Kafka broker with Ranger Kafka plugin on classpath
# Base  : 192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0
# Adds  : Ranger Kafka plugin + all required transitive JARs (2.7.0)
# Target: 192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0-ranger
#
# Multi-stage build:
#   Stage 1 (compiler): JDK 21 image — compiles the patched RangerKafkaAuthorizer
#                       and repacks ranger-kafka-plugin-2.7.0.jar with the fix.
#   Stage 2 (downloader): downloads all Ranger + transitive JARs from Maven Central.
#   Stage 3 (runtime): Strimzi base — copies patched JAR + all deps in.
#
# KRaft combined-mode patch (RangerKafkaAuthorizer):
#   In KRaft combined controller+broker mode Kafka instantiates two separate
#   RangerKafkaAuthorizer objects. The first (controller) sets the static
#   rangerPlugin field AND its own instance auditHandler field. The second
#   (broker) finds rangerPlugin != null, skips the synchronized block in
#   configure(), and leaves its auditHandler null — NPE on every authorize().
#   The patch adds a lazy double-checked init of auditHandler in
#   callRangerPlugin() so the broker instance self-heals on first use.
#
# Authorizer class: org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer
# Config XMLs mounted via KafkaNodePool template.pod.volumes at /mnt/ranger-conf/
# CLASSPATH=/mnt/ranger-conf env var seeds kafka-run-class.sh so ClassLoader
# can find ranger-kafka-security.xml and ranger-kafka-audit.xml.
# =============================================================================

# ── Stage 1: compile patched RangerKafkaAuthorizer ───────────────────────────
# Use the Strimzi base image itself as the compiler context — it has the JRE
# and all Kafka + Ranger JARs already on /opt/kafka/libs/. We install javac
# via the same JDK package that matches the installed JRE.
FROM 192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0-ranger-v3 AS compiler

ARG RANGER_VERSION=2.7.0

USER root

# Install JDK (same major as the installed JRE so javac produces compatible bytecode)
RUN microdnf install -y java-21-openjdk-devel 2>/dev/null \
    || dnf install -y java-21-openjdk-devel 2>/dev/null \
    || apt-get install -y --no-install-recommends openjdk-21-jdk-headless 2>/dev/null \
    || true

WORKDIR /build

# Copy patched source
COPY RangerKafkaAuthorizer.java \
     org/apache/ranger/authorization/kafka/authorizer/RangerKafkaAuthorizer.java

# Compile against the full /opt/kafka/libs classpath (Ranger + Kafka + all deps)
# Find javac from whichever JDK was installed
RUN JAVAC=$(find /usr -name javac -type f 2>/dev/null | head -1) \
    && JAR_TOOL=$(find /usr -name jar -type f 2>/dev/null | head -1) \
    && echo "Using javac: $JAVAC  jar: $JAR_TOOL" \
    && $JAVAC -cp "/opt/kafka/libs/*" \
        -d classes \
        org/apache/ranger/authorization/kafka/authorizer/RangerKafkaAuthorizer.java \
    && echo "Compiled OK" \
    && ls classes/org/apache/ranger/authorization/kafka/authorizer/ \
    && mkdir original \
    && cd original && $JAR_TOOL xf /opt/kafka/libs/ranger-kafka-plugin-${RANGER_VERSION}.jar \
    && cd /build && cp -r classes/org original/ \
    && cd original && $JAR_TOOL cf /build/ranger-kafka-plugin-patched.jar . \
    && echo "JAR repacked, size: $(wc -c < /build/ranger-kafka-plugin-patched.jar) bytes"

# ── Stage 2: download all runtime JARs from Maven Central ────────────────────
FROM eclipse-temurin:21-jdk AS downloader

ARG RANGER_VERSION=2.7.0
ARG HADOOP_VERSION=3.3.6

# Required JARs — full transitive closure of ranger-plugins-common 2.7.0
# (versions from ranger-2.7.0.pom properties + crash log analysis).
# The Strimzi base ships Jersey 3.x (jakarta.ws.rs.*); Ranger 2.7.0 needs
# jersey-bundle 1.19.4 (javax.ws.rs.*) — a completely different package.
#
# Ranger JARs:
#   ranger-kafka-plugin       — RangerKafkaAuthorizer (patched version used instead)
#   ranger-plugins-common     — RangerBasePlugin, RangerPluginConfig, PolicyRefresher
#   ranger-audit-core         — AuditProviderFactory
#   ranger-plugin-classloader — plugin class isolation
#   ranger-plugins-cred       — credential store (loaded by RangerBasePlugin)
# Hadoop:
#   hadoop-common             — org.apache.hadoop.conf.Configuration
#   hadoop-auth               — Hadoop auth (transitive)
#   hadoop-shaded-guava       — shaded Guava (transitive)
# XML:
#   woodstox-core             — XML streaming for RangerBasePlugin
#   stax2-api                 — StAX2 API (woodstox dep)
# Commons:
#   commons-configuration2    — Apache commons config
#   commons-lang              — org.apache.commons.lang.StringUtils (2.x, NOT lang3)
#   commons-text              — org.apache.commons.text (StringSubstitutor etc.)
# Jersey 1.x (JAX-RS 1.x — javax.ws.rs.*):
#   jersey-bundle             — full Jersey 1.19.4 + javax.ws.rs API
# Jackson JAX-RS:
#   jackson-jaxrs-base        — base JAX-RS provider
#   jackson-jaxrs-json-provider — JSON provider wired into Jersey client
# JNA:
#   jna / jna-platform        — JNA bindings (used by gethostname4j)
#   gethostname4j             — host identity for Ranger audit
RUN RANGER="https://repo1.maven.org/maven2/org/apache/ranger" \
    && HADOOP="https://repo1.maven.org/maven2/org/apache/hadoop" \
    && HADOOP_TP="https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty" \
    && COMMONS_LANG="https://repo1.maven.org/maven2/commons-lang" \
    && COMMONS="https://repo1.maven.org/maven2/org/apache/commons" \
    && FASTERXML="https://repo1.maven.org/maven2/com/fasterxml" \
    && JERSEY="https://repo1.maven.org/maven2/com/sun/jersey" \
    && JNA="https://repo1.maven.org/maven2/net/java/dev/jna" \
    && KSTRUCT="https://repo1.maven.org/maven2/com/kstruct" \
    && L="/jars" && mkdir $L \
    && curl -fsSL "${RANGER}/ranger-plugins-common/${RANGER_VERSION}/ranger-plugins-common-${RANGER_VERSION}.jar"         -o "${L}/ranger-plugins-common-${RANGER_VERSION}.jar" \
    && curl -fsSL "${RANGER}/ranger-audit-core/${RANGER_VERSION}/ranger-audit-core-${RANGER_VERSION}.jar"                 -o "${L}/ranger-audit-core-${RANGER_VERSION}.jar" \
    && curl -fsSL "${RANGER}/ranger-plugin-classloader/${RANGER_VERSION}/ranger-plugin-classloader-${RANGER_VERSION}.jar" -o "${L}/ranger-plugin-classloader-${RANGER_VERSION}.jar" \
    && curl -fsSL "${RANGER}/ranger-plugins-cred/${RANGER_VERSION}/ranger-plugins-cred-${RANGER_VERSION}.jar"             -o "${L}/ranger-plugins-cred-${RANGER_VERSION}.jar" \
    && curl -fsSL "${HADOOP}/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar"                         -o "${L}/hadoop-common-${HADOOP_VERSION}.jar" \
    && curl -fsSL "${HADOOP}/hadoop-auth/${HADOOP_VERSION}/hadoop-auth-${HADOOP_VERSION}.jar"                             -o "${L}/hadoop-auth-${HADOOP_VERSION}.jar" \
    && curl -fsSL "${HADOOP_TP}/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar"                                 -o "${L}/hadoop-shaded-guava-1.1.1.jar" \
    && curl -fsSL "${FASTERXML}/woodstox/woodstox-core/5.4.0/woodstox-core-5.4.0.jar"                                    -o "${L}/woodstox-core-5.4.0.jar" \
    && curl -fsSL "https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar"             -o "${L}/stax2-api-4.2.1.jar" \
    && curl -fsSL "${COMMONS}/commons-configuration2/2.10.1/commons-configuration2-2.10.1.jar"                           -o "${L}/commons-configuration2-2.10.1.jar" \
    && curl -fsSL "${COMMONS_LANG}/commons-lang/2.6/commons-lang-2.6.jar"                                                -o "${L}/commons-lang-2.6.jar" \
    && curl -fsSL "${COMMONS}/commons-text/1.10.0/commons-text-1.10.0.jar"                                               -o "${L}/commons-text-1.10.0.jar" \
    && curl -fsSL "${JERSEY}/jersey-bundle/1.19.4/jersey-bundle-1.19.4.jar"                                              -o "${L}/jersey-bundle-1.19.4.jar" \
    && curl -fsSL "${FASTERXML}/jackson/jaxrs/jackson-jaxrs-base/2.17.2/jackson-jaxrs-base-2.17.2.jar"                  -o "${L}/jackson-jaxrs-base-2.17.2.jar" \
    && curl -fsSL "${FASTERXML}/jackson/jaxrs/jackson-jaxrs-json-provider/2.17.2/jackson-jaxrs-json-provider-2.17.2.jar" -o "${L}/jackson-jaxrs-json-provider-2.17.2.jar" \
    && curl -fsSL "${JNA}/jna/5.7.0/jna-5.7.0.jar"                                                                       -o "${L}/jna-5.7.0.jar" \
    && curl -fsSL "${JNA}/jna-platform/5.7.0/jna-platform-5.7.0.jar"                                                     -o "${L}/jna-platform-5.7.0.jar" \
    && curl -fsSL "${KSTRUCT}/gethostname4j/1.0.0/gethostname4j-1.0.0.jar"                                               -o "${L}/gethostname4j-1.0.0.jar"

# ── Stage 3: runtime image ────────────────────────────────────────────────────
FROM 192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0

USER root

COPY --from=compiler /build/ranger-kafka-plugin-patched.jar /opt/kafka/libs/ranger-kafka-plugin-2.7.0.jar
COPY --from=downloader /jars/ /opt/kafka/libs/

USER 1000
