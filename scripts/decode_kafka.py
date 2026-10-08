import sys, struct, io
sys.path.insert(0, '/opt/spark/work-dir')
from bao_spark_init import BaoSparkInit
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
import avro.io as aio, avro.schema as aschema

bao = BaoSparkInit()
ks  = bao.kafka_creds()
cfg = {
    'bootstrap.servers': 'strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism':    'SCRAM-SHA-512',
    'sasl.username':     ks.get('debezium_user', 'debezium-user'),
    'sasl.password':     ks.get('debezium_password', ''),
    'group.id':          'diag-decode',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'false',
}
c = Consumer(cfg)
sr = SchemaRegistryClient({'url': 'http://schema-registry.prod.svc.cluster.local:8081'})

targets = [
    ('oracle.CACHE_TESTING.CUSTOMERS',          0, 101285, 'ORA'),
    ('mongodb.cache_testing.customers',          0, 124434, 'MDB'),
    ('postgres.cache_testing.public.customers',  0, 5,      'PG '),
]

for topic, partition, start_offset, label in targets:
    print('--- ' + label + ' ' + topic + ' from offset ' + str(start_offset) + ' ---')
    tp = TopicPartition(topic, partition, start_offset)
    c.assign([tp])
    count = 0
    for _ in range(8):
        m = c.poll(2.0)
        if not m or m.error():
            break
        raw = m.value()
        if not raw:
            print('  offset=' + str(m.offset()) + ' TOMBSTONE')
            continue
        if len(raw) > 5 and raw[0] == 0:
            schema_id = struct.unpack('>I', raw[1:5])[0]
            try:
                schema_def = aschema.parse(sr.get_schema(schema_id).schema_str)
                reader = aio.DatumReader(schema_def)
                record = reader.read(aio.BinaryDecoder(io.BytesIO(raw[5:])))
                op    = record.get('op', '?')
                after = record.get('after') or {}
                src   = record.get('source') or {}
                pk    = after.get('CUSTOMER_ID') or after.get('customer_id') or after.get('id', '?')
                tier  = after.get('TIER') or after.get('tier', '?')
                ts_ms = src.get('ts_ms', '?')
                print('  offset=' + str(m.offset()) + ' op=' + str(op) + ' pk=' + str(pk) + ' tier=' + str(tier) + ' ts_ms=' + str(ts_ms))
                count += 1
            except Exception as e:
                print('  offset=' + str(m.offset()) + ' DECODE_ERR=' + str(e))
        else:
            try:
                print('  offset=' + str(m.offset()) + ' JSON=' + raw[:80].decode())
            except Exception:
                print('  offset=' + str(m.offset()) + ' bytes=' + str(raw[:10]))
    if count == 0:
        print('  (no messages decoded)')

c.close()
