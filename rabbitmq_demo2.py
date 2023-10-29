import pika
import time
import threading
import sqlite3


class BaseConsumer:
    def __init__(self, username, password, rabbitmq_server_ip, queue_name, db_path):
        self.credentials = pika.PlainCredentials(username, password)
        self.rabbitmq_server_ip = rabbitmq_server_ip
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.db_path = db_path

    def connect(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(self.rabbitmq_server_ip, credentials=self.credentials))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def callback(self, ch, method, properties, body):
        raise NotImplementedError

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
    def callback(self, ch, method, properties, body):
        print("Received %r" % body)

        # 连接到SQLite数据库
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        # 创建表
        c.execute('''CREATE TABLE IF NOT EXISTS messages
                     (body text)''')

        # 插入一行记录
        c.execute("INSERT INTO messages VALUES (?)", (body.decode('utf-8'),))

        # 提交事务
        conn.commit()

        # 关闭连接
        conn.close()

        print("Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)


# 使用示例
consumer = MyConsumer('username', 'password', 'rabbitmq_server_ip', 'queue_name', 'mydatabase.db')

# 创建一个线程来运行消费者
thread = threading.Thread(target=consumer.consume)
thread.start()
