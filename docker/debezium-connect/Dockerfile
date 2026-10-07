# =============================================================================
# debezium-connect — extends the upstream Debezium Kafka Connect 2.7 image with:
#   1. A custom LowerCaseTopicNamingStrategy plugin JAR (Oracle topic naming).
#   2. Confluent Avro converter JARs (kafka-avro-serializer + dependencies)
#      so connectors can use AvroConverter with the Schema Registry.
#
# Why the lowercase JAR is needed:
#   Oracle stores identifiers in uppercase in its data dictionary.
#   The default Debezium DefaultTopicNamingStrategy therefore produces topics
#   like  oracle.CACHE_TESTING.CUSTOMERS  which conflicts with our streaming
#   pipeline that expects lowercase  oracle.cache_testing.customers.
#   Debezium 2.7.x does not ship a built-in lowercase strategy, so we bundle
#   one as a plugin alongside the oracle connector JARs.
#
# Why the Avro converter JARs are needed:
#   The upstream Debezium image ships only connector JARs, not the Confluent
#   io.confluent.connect.avro.AvroConverter.  We download the required JARs
#   at build time from Maven Central (no Confluent registry login required).
#   Versions pinned to match Confluent Platform 7.6.x (kafka-avro-serializer
#   7.6.0 is compatible with Debezium 2.7 / Kafka 3.7).
#
# JAR sources:
#   docker/debezium-connect/LowerCaseTopicNamingStrategy.java
#   docker/debezium-connect/debezium-lowercase-topic-naming-2.7.4.jar
#
# Tags:
#   192.168.1.50:30500/debezium/connect:2.7       (floating)
#   192.168.1.50:30500/debezium/connect:2.7.4-lc1 (pinned, lc = lowercase)
# =============================================================================
FROM quay.io/debezium/connect:2.7

# ── 1. LowerCaseTopicNamingStrategy ──────────────────────────────────────────
COPY debezium-lowercase-topic-naming-2.7.4.jar \
     /kafka/connect/debezium-connector-oracle/debezium-lowercase-topic-naming-2.7.4.jar

# ── 2. Confluent Avro converter ───────────────────────────────────────────────
# kafka-avro-serializer and its transitive runtime deps from Maven Central.
# Placed in a dedicated plugin directory so Kafka Connect picks them up via
# plugin.path=/kafka/connect.
ENV CONFLUENT_VERSION=7.6.0

# Download the Confluent Hub self-contained zip for kafka-connect-avro-converter.
# The zip bundles every transitive dep in lib/ so no manual dep-chasing is needed.
RUN mkdir -p /kafka/connect/confluent-avro-converter && \
    cd /tmp && \
    curl -fsSL \
      "https://d2p6pa21dvn84.cloudfront.net/api/plugins/confluentinc/kafka-connect-avro-converter/versions/${CONFLUENT_VERSION}/confluentinc-kafka-connect-avro-converter-${CONFLUENT_VERSION}.zip" \
      -o avro-converter.zip && \
    unzip -q avro-converter.zip -d /tmp/avro-converter-unpacked && \
    cp /tmp/avro-converter-unpacked/confluentinc-kafka-connect-avro-converter-${CONFLUENT_VERSION}/lib/*.jar \
       /kafka/connect/confluent-avro-converter/ && \
    rm -rf /tmp/avro-converter.zip /tmp/avro-converter-unpacked && \
    echo "Confluent Avro converter JARs installed:" && ls -lh /kafka/connect/confluent-avro-converter/
