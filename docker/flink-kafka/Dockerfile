FROM 192.168.1.50:30500/apache/flink:2.0.0-java17
# Add flink-sql-connector-kafka so the Flink K8s Operator 1.16.1 ENABLE_BUILT_IN_PLUGINS works.
# The operator injects ENABLE_BUILT_IN_PLUGINS=flink-sql-connector-kafka-2.0.0.jar;
# docker-entrypoint.sh expects the jar at /opt/flink/opt/flink-sql-connector-kafka-2.0.0.jar.
COPY flink-sql-connector-kafka-2.0.0.jar /opt/flink/opt/flink-sql-connector-kafka-2.0.0.jar
