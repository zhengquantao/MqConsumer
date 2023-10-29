import threading
from confluent_kafka import Consumer, Producer, KafkaException

class KafkaConsumer:
    def __init__(self, kafka_config):
        self.kafka_config = kafka_config
        self.consumer = Consumer(self.kafka_config)
        self.producer = Producer(self.kafka_config)

    def consume(self):
        try:
            self.consumer.subscribe(['my_input_topic'])

            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break

                print('Received message: {}'.format(msg.value().decode('utf-8')))

                # 将消费完的数据重新投递至Kafka
                self.producer.produce('my_output_topic', msg.value())
                self.producer.flush()

        except KeyboardInterrupt:
            pass

        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

# 使用示例
kafka_config = {'bootstrap.servers': 'localhost:9092', 'group.id': 'my_group', 'auto.offset.reset': 'earliest'}
consumer = KafkaConsumer(kafka_config)

# 创建线程来运行消费者
thread = threading.Thread(target=consumer.consume)
thread.start()