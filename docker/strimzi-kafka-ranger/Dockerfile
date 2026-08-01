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
# Required JARs (confirmed by crash analysis):
#   ranger-kafka-plugin       — authorizer + policy evaluation
#   ranger-plugins-common     — base plugin framework
#   ranger-audit-core         — audit event dispatch
#   ranger-plugin-classloader — class isolation
#   hadoop-common             — org.apache.hadoop.conf.Configuration
#   hadoop-auth               — Hadoop authentication (transitive of hadoop-common)
#   hadoop-shaded-guava       — shaded Guava used by Hadoop (transitive)
#   woodstox-core             — XML streaming (RangerBasePlugin reads security.xml)
#   stax2-api                 — StAX2 API (woodstox dependency)
#   commons-configuration2    — commons config (hadoop-common dependency)
#   commons-lang              — org.apache.commons.lang.StringUtils used by RangerPluginConfig
#                               NOTE: this is commons-lang 2.x (org.apache.commons.lang),
#                               NOT commons-lang3 (org.apache.commons.lang3). Both have
#                               different Maven coordinates and are not interchangeable.
RUN RANGER="https://repo1.maven.org/maven2/org/apache/ranger" \
    && HADOOP="https://repo1.maven.org/maven2/org/apache/hadoop" \
    && COMMONS="https://repo1.maven.org/maven2/commons-lang" \
    && COMMONS_APACHE="https://repo1.maven.org/maven2/org/apache/commons" \
    && FASTERXML="https://repo1.maven.org/maven2/com/fasterxml/woodstox" \
    && CODEHAUS="https://repo1.maven.org/maven2/org/codehaus/woodstox" \
    && HADOOP_TP="https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty" \
    && L="/opt/kafka/libs" \
    && curl -fsSL "${RANGER}/ranger-kafka-plugin/${RANGER_VERSION}/ranger-kafka-plugin-${RANGER_VERSION}.jar"             -o "${L}/ranger-kafka-plugin-${RANGER_VERSION}.jar" \
    && curl -fsSL "${RANGER}/ranger-plugins-common/${RANGER_VERSION}/ranger-plugins-common-${RANGER_VERSION}.jar"         -o "${L}/ranger-plugins-common-${RANGER_VERSION}.jar" \
    && curl -fsSL "${RANGER}/ranger-audit-core/${RANGER_VERSION}/ranger-audit-core-${RANGER_VERSION}.jar"                 -o "${L}/ranger-audit-core-${RANGER_VERSION}.jar" \
    && curl -fsSL "${RANGER}/ranger-plugin-classloader/${RANGER_VERSION}/ranger-plugin-classloader-${RANGER_VERSION}.jar" -o "${L}/ranger-plugin-classloader-${RANGER_VERSION}.jar" \
    && curl -fsSL "${HADOOP}/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar"                         -o "${L}/hadoop-common-${HADOOP_VERSION}.jar" \
    && curl -fsSL "${HADOOP}/hadoop-auth/${HADOOP_VERSION}/hadoop-auth-${HADOOP_VERSION}.jar"                             -o "${L}/hadoop-auth-${HADOOP_VERSION}.jar" \
    && curl -fsSL "${HADOOP_TP}/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar"                                 -o "${L}/hadoop-shaded-guava-1.1.1.jar" \
    && curl -fsSL "${FASTERXML}/woodstox-core/5.4.0/woodstox-core-5.4.0.jar"                                             -o "${L}/woodstox-core-5.4.0.jar" \
    && curl -fsSL "${CODEHAUS}/stax2-api/4.2.1/stax2-api-4.2.1.jar"                                                      -o "${L}/stax2-api-4.2.1.jar" \
    && curl -fsSL "${COMMONS_APACHE}/commons-configuration2/2.10.1/commons-configuration2-2.10.1.jar"                    -o "${L}/commons-configuration2-2.10.1.jar" \
    && curl -fsSL "${COMMONS}/commons-lang/2.6/commons-lang-2.6.jar"                                                     -o "${L}/commons-lang-2.6.jar"

USER 1000
