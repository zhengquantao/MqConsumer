import kombu

from engine_process import ProcessEngine
from engine_save import MySQLResultSaver


class SubDomainEngine(ProcessEngine):

    def __init__(self, connection, queue, exchange, routing_key, result_saver):
        super().__init__(connection, queue, exchange, routing_key, result_saver)

    def handle_task_data(self, data):
        pass


class DomainEngine(ProcessEngine):

    def __init__(self, connection, queue, exchange, routing_key, result_saver):
        super().__init__(connection, queue, exchange, routing_key, result_saver)

    def handle_task_data(self, data):
        print(data)
        return {}


class WebFingerEngine(ProcessEngine):

    def __init__(self, connection, queue, exchange, routing_key, result_saver):
        super().__init__(connection, queue, exchange, routing_key, result_saver)

    def handle_task_data(self, data):
        pass


# 创建一个连接对象，指定传输方式、主机、端口、用户名、密码等参数
connection = kombu.Connection(transport="amqp",
                              hostname="localhost",
                              port=5672,
                              userid="guest",
                              password="guest")

# 创建一个交换机对象，指定名称、类型和持久化等参数
exchange = kombu.Exchange(name="test",
                          type="direct",
                          durable=True)

# 创建一个队列对象，指定名称、交换机和路由键等参数，并绑定到交换机上
queue = kombu.Queue(name="test",
                    exchange=exchange,
                    routing_key="test")
queue.bind(connection)


# 在创建引擎框架实例时，传入一个结果保存对象作为参数
result_saver = MySQLResultSaver()  # 创建一个MySQL结果保存对象
domain_engine = DomainEngine(connection, queue, exchange, "test_key", result_saver)  # 将结果保存对象传入引擎框架实例



# 调用下游子类的网络连接失败重试的方法，确保连接成功
domain_engine.run()



def test_producer():
    # 创建连接
    with Connection(transport="amqp",
                    hostname="localhost",
                    port=5672,
                    userid="guest",
                    password="guest") as conn:
        # 创建交换机
        exchange = Exchange("test", type="direct")

        # 创建生产者
        producer = conn.Producer(serializer='json')

        # 创建队列
        queue = Queue(name="test", exchange=exchange, routing_key="test")

        # 确保队列已经存在
        queue.maybe_bind(conn)
        queue.declare()

        # 发送消息
        message = {"data": {"env": True, "debug": 8, "msg": "hello world8"}}
        producer.publish(message, exchange=exchange, serializer='json', compression='bzip2', routing_key="test",
                         retry=True)

test_producer()


# 如何使用Python kombu连接rabbitmq队列？
#
# 您好，这是Bing。要使用Python kombu连接到rabbitmq队列，您可以按照以下步骤操作：
#
# 首先，您需要安装kombu模块。您可以使用pip install kombu命令来安装。
#
# 然后，您需要创建一个连接对象，指定传输方式、主机、端口、用户名、密码等参数。例如：


from kombu import Connection, Consumer

connection = Connection(transport="amqp", hostname="localhost", port=5672, userid="guest", password="guest")
# 接下来，您需要创建一个交换机对象，指定名称、类型和持久化等参数。例如：


from kombu import Exchange
exchange = Exchange(name="test_exchange", type="direct", durable=True)
# 然后，您需要创建一个队列对象，指定名称、交换机和路由键等参数，并绑定到交换机上。例如：

from kombu import Queue
queue = Queue(name="test_queue", exchange=exchange, routing_key="test_key")
queue.bind(connection)
# 最后，您可以创建一个消费者对象，订阅队列，并处理收到的消息。例如：

from kombu.mixins import ConsumerProducerMixin


class Worker(ConsumerProducerMixin):
    def __init__(self, connection, queues):
        self.connection = connection
        self.queues = queues

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.queues, callbacks=[self.on_message])]

    def on_message(self, body, message):
        print(f'Received message: {body}')
        message.ack()

worker = Worker(connection, [queue])
worker.run()
# 以上代码会创建一个消费者对象，并开始监听队列中的消息。当收到消息时，它会打印消息内容，并确认消息已经处理完成1。





from kombu import Connection
connection = Connection(transport="redis", hostname="localhost", port=6379)
# 接下来，您需要创建一个交换机对象，指定名称、类型和持久化等参数。例如：

from kombu import Exchange
exchange = Exchange(name="test_exchange", type="direct", durable=True)
# 然后，您需要创建一个队列对象，指定名称、交换机和路由键等参数，并绑定到交换机上。例如：

from kombu import Queue
queue = Queue(name="test_queue", exchange=exchange, routing_key="test_key")
queue.bind(connection)
# 最后，您可以创建一个消费者对象，订阅队列，并处理收到的消息。例如：


from kombu.mixins import ConsumerProducerMixin

class Worker(ConsumerProducerMixin):
    def __init__(self, connection, queues):
        self.connection = connection
        self.queues = queues

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.queues, callbacks=[self.on_message])]

    def on_message(self, body, message):
        print(f'Received message: {body}')
        message.ack()

worker = Worker(connection, [queue])
worker.run()
# 以上代码会创建一个消费者对象，并开始监听队列中的消息。当收到消息时，它会打印消息内容，并确认消息已经处理完成1。



from kombu import Connection, Exchange, Queue, Consumer, Producer
import time

# 定义连接、交换机、队列和路由键
rabbit_url = "amqp://guest:guest@localhost:5672//"
conn = Connection(rabbit_url)
exchange = Exchange("my-exchange", type="direct")
queue = Queue(name="my-queue", exchange=exchange, routing_key="test_key")
second_queue = Queue(name="second-queue", exchange=exchange, routing_key="second_key")

# 消息处理函数
def process_message(body, message):
    print("Received message: {0!r}".format(body))
    try:
        # 在这里处理你的消息
        # 如果处理成功，确认消息并将其重新投递到其他队列中
        message.ack()
        with Producer(conn) as producer:
            producer.publish(body,
                             exchange=exchange,
                             routing_key="second_key",
                             declare=[second_queue])
    except Exception as e:
        print("Error processing message: {0!r}".format(e))
        # 如果处理失败，根据错误类型决定是否重试
        if isinstance(e, TemporaryError):
            # 如果是临时错误，例如网络问题，那么重试
            message.requeue()
        else:
            # 如果是其他类型的错误，那么拒绝消息并将其发送到死信队列
            message.reject(requeue=False)

# 消费者
with Consumer(conn, queues=queue, callbacks=[process_message], accept=["text/plain"]):
    while True:
        try:
            conn.drain_events(timeout=2)  # 设置超时时间
        except conn.connection_errors:
            print("Connection error, trying to reconnect...")
            time.sleep(2)  # 如果连接失败，等待一段时间再重试