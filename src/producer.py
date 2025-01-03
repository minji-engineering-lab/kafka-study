from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
import json
import msgpack

log = logging.getLogger()

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('err', exc_info=excp)
    # handle exception
    
producer = KafkaProducer(bootstrap_servers=[])
future = producer.send('my-topic', b'raw-bytes')

try:
    record_metadata = future.get(timeout=10)
    on_send_success(record_metadata)
except KafkaError:
    log.exception()
    pass

# produce keyed messages to enable hashed partitioning
producer.send('my-topic', key=b'foo', value=b'bar')

# encode objects via msgpack
#J SON처럼 데이터를 키-값 형태로 직렬화하지만, 더 작은 크기의 바이너리 형식으로 변환하여 속도와 효율성을 높입니다.
producer = KafkaProducer(value_serializer=msgpack.dumps)
producer.send('msgpack-topic', {'key':'value'})

# produce json messages 
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
producer.send('json-topic', {'key': 'value'})

for i in range(100):
    producer.send('my-topic', f'hello this is event {i}!')

producer.send('my-topic', b'raw_bytes')\
    .add_callback(on_send_success)\
    .add_errorback(on_send_error)
producer.flush()
