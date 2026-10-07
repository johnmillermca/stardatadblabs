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
ENV CONFLUENT_VERSION=7.6.0 \
    AVRO_VERSION=1.11.3

RUN mkdir -p /kafka/connect/confluent-avro-converter && \
    cd /kafka/connect/confluent-avro-converter && \
    # kafka-connect-avro-converter (the AvroConverter class itself)
    curl -fsSL "https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/${CONFLUENT_VERSION}/kafka-connect-avro-converter-${CONFLUENT_VERSION}.jar" \
         -o "kafka-connect-avro-converter-${CONFLUENT_VERSION}.jar" && \
    # kafka-avro-serializer
    curl -fsSL "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/${CONFLUENT_VERSION}/kafka-avro-serializer-${CONFLUENT_VERSION}.jar" \
         -o "kafka-avro-serializer-${CONFLUENT_VERSION}.jar" && \
    # kafka-schema-registry-client
    curl -fsSL "https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/${CONFLUENT_VERSION}/kafka-schema-registry-client-${CONFLUENT_VERSION}.jar" \
         -o "kafka-schema-registry-client-${CONFLUENT_VERSION}.jar" && \
    # kafka-schema-serializer (contains AbstractKafkaSchemaSerDeConfig)
    curl -fsSL "https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/${CONFLUENT_VERSION}/kafka-schema-serializer-${CONFLUENT_VERSION}.jar" \
         -o "kafka-schema-serializer-${CONFLUENT_VERSION}.jar" && \
    # kafka-connect-avro-data (AvroData helper used by AvroConverter)
    curl -fsSL "https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/${CONFLUENT_VERSION}/kafka-connect-avro-data-${CONFLUENT_VERSION}.jar" \
         -o "kafka-connect-avro-data-${CONFLUENT_VERSION}.jar" && \
    # Apache Avro runtime
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/avro/avro/${AVRO_VERSION}/avro-${AVRO_VERSION}.jar" \
         -o "avro-${AVRO_VERSION}.jar" && \
    # jackson-databind (Avro JSON encoder dep — already in base image but pin to be safe)
    curl -fsSL "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.3/jackson-databind-2.15.3.jar" \
         -o "jackson-databind-2.15.3.jar" && \
    # guava (CachedSchemaRegistryClient uses Ticker from com.google.common.base)
    curl -fsSL "https://repo1.maven.org/maven2/com/google/guava/guava/32.1.3-jre/guava-32.1.3-jre.jar" \
         -o "guava-32.1.3-jre.jar" && \
    echo "Confluent Avro converter JARs installed:" && ls -lh .
