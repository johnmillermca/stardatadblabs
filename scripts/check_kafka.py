import sys
sys.path.insert(0, '/opt/spark/work-dir')
from bao_spark_init import BaoSparkInit
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

bao = BaoSparkInit()
ks  = bao.kafka_creds()
cfg = {
    'bootstrap.servers': 'strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism':    'SCRAM-SHA-512',
    'sasl.username':     ks.get('debezium_user', 'debezium-user'),
    'sasl.password':     ks.get('debezium_password', ''),
    'group.id':          'e2e-kafka-check',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'false',
}
c    = Consumer(cfg)
sr   = SchemaRegistryClient({'url': 'http://schema-registry.prod.svc.cluster.local:8081'})
desr = AvroDeserializer(sr)

TEST_PKS = {'88880001', '88880002', '88880003'}
found = {}

topics = [
    'postgres.cache_testing.public.customers',
    'oracle.CACHE_TESTING.CUSTOMERS',
    'mongodb.cache_testing.customers',
]

for topic in topics:
    parts = sorted(c.list_topics(topic, timeout=5).topics[topic].partitions.keys())
    for p in parts:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=5)
        c.assign([TopicPartition(topic, p, max(lo, hi - 15))])
        for _ in range(20):
            m = c.poll(1.5)
            if not m or m.error():
                break
            try:
                rec   = desr(m.value(), SerializationContext(topic, MessageField.VALUE))
                op    = rec.get('op', '?')
                after = rec.get('after') or {}
                raw_pk = after.get('CUSTOMER_ID') or after.get('customer_id') or after.get('id')
                pk_str = str(raw_pk).rstrip('.0') if raw_pk is not None else None
                if pk_str in TEST_PKS:
                    tier = after.get('TIER') or after.get('tier', '?')
                    src  = (rec.get('source') or {}).get('ts_ms', '?')
                    print('KAFKA_OK  topic=' + topic.split('.')[-1] +
                          ' p=' + str(p) + ' offset=' + str(m.offset()) +
                          ' op=' + str(op) + ' pk=' + pk_str +
                          ' tier=' + str(tier) + ' ts_ms=' + str(src))
                    found[pk_str] = True
            except Exception:
                pass

c.close()

for pk in sorted(TEST_PKS):
    if pk not in found:
        print('KAFKA_MISSING  pk=' + pk)

if len(found) == len(TEST_PKS):
    print('RESULT: ALL 3 PKs confirmed in Kafka')
else:
    print('RESULT: Only ' + str(len(found)) + '/3 found: ' + str(list(found.keys())))
