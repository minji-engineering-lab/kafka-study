from kafka import KafkaConsumer

import msgpack
import json 

consumer = KafkaConsumer('my-topic',
                        group_id = 'my-group',
                        bootstrap_servers=['localhost:9092'])

for message in consumer:
    print("%s:%d:%d key=%s value=%s" % (message.topic, message.partition,
                                        message.offset, message.key,
                                        message.value))

# consume earliest available messages, don't commit offsets
KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)
# consume json messages
KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))
# consume msgpack
KafkaConsumer(value_deserializer=msgpack.unpackb)
KafkaConsumer(consumer_timeout_ms=1000)