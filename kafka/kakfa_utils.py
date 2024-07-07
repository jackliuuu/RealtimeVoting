import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
from confluent_kafka.serialization import StringSerializer, StringDeserializer

class kafkaUtils:
    def __init__(self, conf: dict, group_id: str):
        self.consumer_conf = conf | {
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.consumer = Consumer(self.consumer_conf)
        self.producer_conf = {
        'bootstrap.servers': 'localhost:9092',
        }
        self.producer = SerializingProducer(self.producer_conf)

    def produce_message(self, topic: str, key: str, value: dict, on_delivery):
        self.producer.produce(
            topic,
            key=key,
            value=json.dumps(value),
            on_delivery=on_delivery
        )
        self.producer.poll(0)
        self.producer.flush()

    def consume_messages(self, topic: str, max_messages: int = 100):
        result = []
        self.consumer.subscribe([topic])
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break
                else:
                    result.append(json.loads(msg.value().decode('utf-8')))
                    self.consumer.commit()
                    if len(result) == max_messages:
                        return result
            self.consumer.close()
        except KafkaException as e:
            print(e)

    def topic_is_empty(self, topic: str) -> bool:
        self.consumer.subscribe([topic])
        msg = self.consumer.poll(timeout=1.0)
        self.consumer.unsubscribe()
        return msg is None

