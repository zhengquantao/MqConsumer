import asyncio
import aiomysql
import pika
import time
import threading

class BaseConsumer:
    def __init__(self, username, password, rabbitmq_server_ip, queue_name):
        self.credentials = pika.PlainCredentials(username, password)
        self.rabbitmq_server_ip = rabbitmq_server_ip
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.rabbitmq_server_ip, credentials=self.credentials))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    async def process_data(self, data):
        raise NotImplementedError

    def callback(self, ch, method, properties, body):
        print("Received %r" % body)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.process_data(body))
        print("Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def consume(self):
        while True:
            try:
                self.connect()
                self.channel.basic_consume(self.callback, queue=self.queue_name)
                print('Waiting for messages. To exit press CTRL+C')
                self.channel.start_consuming()
            except pika.exceptions.AMQPConnectionError:
                print("Connection was closed, retrying...")
                time.sleep(5)

class MyConsumer(BaseConsumer):
    def __init__(self, username, password, rabbitmq_server_ip, queue_name, db_config):
        super().__init__(username, password, rabbitmq_server_ip, queue_name)
        self.db_config = db_config

    async def process_data(self, data):
        # 创建连接池
        pool = await aiomysql.create_pool(**self.db_config)

        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # 插入一行记录
                await cur.execute("INSERT INTO messages VALUES (%s)", (data.decode('utf-8'),))

                # 提交事务
                await conn.commit()

# 使用示例
db_config = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'username',
    'password': 'password',
    'db': 'mydatabase',
    'loop': asyncio.get_event_loop(),
}
consumer = MyConsumer('username', 'password', 'rabbitmq_server_ip', 'queue_name', db_config)

# 创建一个线程来运行消费者
thread = threading.Thread(target=consumer.consume)
thread.start()
