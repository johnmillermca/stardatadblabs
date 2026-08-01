# =============================================================================
# Strimzi Kafka broker with Ranger Kafka plugin on classpath
# Base  : 192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0
# Adds  : Ranger Kafka plugin + all required transitive JARs (2.7.0)
# Target: 192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0-ranger
#
# Multi-stage build: JAR deps downloaded from Maven Central in stage 1,
# then copied into the Strimzi base image in stage 2.
#
# The Ranger Kafka plugin provides topic-level access control enforced inside
# the broker JVM. Policies are downloaded from Ranger Admin every 30s and
# cached locally — no network call per message.
#
# Authorizer class: org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer
# Config files mounted via KafkaNodePool template.pod.volumes at /mnt/ranger-conf/
# =============================================================================

FROM 192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0

USER root

ARG RANGER_VERSION=2.7.0
ARG HADOOP_VERSION=3.3.6

# Download Ranger Kafka plugin JARs + all required transitive dependencies.
# The Strimzi base image already has curl available.
#
# Required JARs — full transitive closure of ranger-plugins-common 2.7.0
# (derived from ranger-2.7.0.pom property values + crash log analysis).
# The Strimzi base ships Jersey 3.x (jakarta.ws.rs.*); Ranger 2.7.0 needs
# jersey-bundle 1.19.4 (javax.ws.rs.*) — a completely different package.
#
# Ranger JARs:
#   ranger-kafka-plugin       — RangerKafkaAuthorizer
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
#   jersey-bundle             — full Jersey 1.19.4 client + server + javax.ws.rs API
# Jackson JAX-RS:
#   jackson-jaxrs-base        — base JAX-RS provider
#   jackson-jaxrs-json-provider — JSON provider wired into Jersey client
# JNA (used by gethostname4j):
#   jna                       — JNA core
#   jna-platform              — JNA platform bindings
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
    && L="/opt/kafka/libs" \
    && curl -fsSL "${RANGER}/ranger-kafka-plugin/${RANGER_VERSION}/ranger-kafka-plugin-${RANGER_VERSION}.jar"             -o "${L}/ranger-kafka-plugin-${RANGER_VERSION}.jar" \
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

USER 1000
