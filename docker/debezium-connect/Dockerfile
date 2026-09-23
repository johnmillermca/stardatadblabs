# =============================================================================
# debezium-connect — extends the upstream Debezium Kafka Connect 2.7 image with
# a custom LowerCaseTopicNamingStrategy plugin JAR.
#
# Why this JAR is needed:
#   Oracle stores identifiers in uppercase in its data dictionary.
#   The default Debezium DefaultTopicNamingStrategy therefore produces topics
#   like  oracle.CACHE_TESTING.CUSTOMERS  which conflicts with our streaming
#   pipeline that expects lowercase  oracle.cache_testing.customers.
#   Debezium 2.7.x does not ship a built-in lowercase strategy, so we bundle
#   one as a plugin alongside the oracle connector JARs.
#
# The JAR source lives at:
#   docker/debezium-connect/LowerCaseTopicNamingStrategy.java
# The pre-built JAR lives at:
#   docker/debezium-connect/debezium-lowercase-topic-naming-2.7.4.jar
#
# Tags:
#   192.168.1.50:30500/debezium/connect:2.7       (floating)
#   192.168.1.50:30500/debezium/connect:2.7.4-lc1 (pinned, lc = lowercase)
# =============================================================================
FROM quay.io/debezium/connect:2.7

COPY debezium-lowercase-topic-naming-2.7.4.jar \
     /kafka/connect/debezium-connector-oracle/debezium-lowercase-topic-naming-2.7.4.jar
