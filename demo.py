# 用Python kombu实现一个消费者引擎框架。
# 1.引擎框架具有连接队列，网络连接失败重试、处理消费信息、消费信息失败重试、消费信息结果入库、消费信息结果再发布等功能。引擎框架 处理消费信息功能对下游子类开放。
# 2.下游子类只需要重写 处理消费信息功能，在处理消费信息功能中 实现本身业务的代码逻辑。子类不需要关注连接队列，网络连接失败重试、消费信息失败重试、消费信息结果入库、消费信息结果再发布等功能。
#
# 3.引擎框架可以选择连接指定队列。如：rabbitmq、kafka、redis等。上层的队列逻辑改变，不影响下层的子类代码

# 导入kombu模块
import kombu

from kombu.mixins import ConsumerProducerMixin


# 定义一个中间件类，这是一个抽象类，定义了一些抽象方法
class Middleware:

    def get_consumers(self, consumer, channel):
        raise NotImplementedError

    def on_message(self, *args, **kwargs):
        raise NotImplementedError

    def ensure_connection(self):
        raise NotImplementedError

    def ensure_consume(self):
        raise NotImplementedError

    def save_result(self, result):
        raise NotImplementedError

    def republish_result(self, result):
        raise NotImplementedError

    def run(self):
        raise NotImplementedError

# 定义一个结果保存类，这是一个抽象类，定义了一个抽象方法save
class ResultSaver:
    def save(self, result):
        raise NotImplementedError


# 定义一个MySQL结果保存类，继承自结果保存类
class MySQLResultSaver(ResultSaver):
    def save(self, result):
        # 这里是保存到MySQL数据库的代码
        pass


# 定义一个Redis结果保存类，继承自结果保存类
class RedisResultSaver(ResultSaver):
    def save(self, result):
        # 这里是保存到Redis数据库的代码
        pass


# 定义一个PostgreSQL结果保存类，继承自结果保存类
class PostgreSQLResultSaver(ResultSaver):
    def save(self, result):
        # 这里是保存到PostgreSQL数据库的代码
        pass


# 定义一个MongoDB结果保存类，继承自结果保存类
class MongoDBResultSaver(ResultSaver):
    def save(self, result):
        # 这里是保存到MongoDB数据库的代码
        pass


# 定义一个RabbitMQ中间件类，继承自中间件类
class RabbitMQMiddleware(Middleware, ConsumerProducerMixin):
    # 这里是RabbitMQ中间件的实现代码
    # 初始化方法，接受连接、队列、交换机和路由键作为参数
    def __init__(self, connection, queue, exchange, routing_key, result_saver):
        # # 保存参数到实例属性中
        self.connection = connection
        self.queue = queue
        self.exchange = exchange
        self.routing_key = routing_key
        self.result_saver = result_saver

    # 定义一个连接队列的方法，返回一个kombu.Consumer对象
    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.queue,
                         on_message=self.on_message,
                         accept={"application/json"})]

    # 定义一个处理消费信息的方法，这是一个抽象方法，需要在子类中重写
    def on_message(self, body, message):
        raise NotImplementedError

    # 定义一个网络连接失败重试的方法，使用kombu的ensure_connection装饰器
    @kombu.utils.retries(max_retries=3)
    def ensure_connection(self):
        return self.connection.ensure_connection()

    # 定义一个消费信息失败重试的方法，使用kombu的ensure方法
    def ensure_consume(self):
        return self.connection.ensure(self, self.run)

    # 定义一个消费信息结果入库的方法，这里只是打印结果，可以根据需要修改
    def save_result(self, result):
        self.result_saver.save(result)  # 调用结果保存对象的save方法
        print(f"Result saved: {result}")

    # 定义一个消费信息结果再发布的方法，使用kombu的Producer对象发送消息到交换机和路由键
    def republish_result(self, result):
        with kombu.Producer(self) as producer:
            producer.publish(result,
                             exchange=self.exchange,
                             routing_key=self.routing_key,
                             serializer="json")

    def run(self):
        with Consumer(self.connection, queues=queue, callbacks=[self.on_message],
                      accept=["text/plain", "application/json"]):
            while True:
                try:
                    self.connection.drain_events(timeout=2)  # 设置超时时间
                except self.connection.connection_errors:
                    print("Connection error, trying to reconnect...")
                    time.sleep(2)  # 如果连接失败，等待一段时间再重试


# 定义一个Redis中间件类，继承自中间件类
class RedisMiddleware(Middleware):
    # 这里是Redis中间件的实现代码
    pass


# 定义流程引擎框架类
class ProcessEngine(RabbitMQMiddleware):
    # 重写处理消费信息的方法，在这里实现本身业务的代码逻辑
    def on_message(self, body, message):
        try:
            # 获取消息体中的数据
            payload = body["data"]
            # 对数据进行一些处理，这里只是简单地加一
            result = self.handle_task_data(payload)
            # 调用父类的消费信息结果入库的方法
            self.save_result(result)
            # 调用父类的消费信息结果再发布的方法
            self.republish_result(result)
            # 确认消息已经处理完成
            message.ack()
        except Exception as e:
            # 如果发生异常，打印错误信息，并拒绝消息，让其重新入队列
            print(f"Error: {e}")
            message.reject(requeue=True)

    def handle_task_data(self, data):
        raise NotImplementedError


class SubDomainEngine(ProcessEngine):

    def __init__(self, connection, queue, exchange, routing_key, result_saver):
        super().__init__(connection, queue, exchange, routing_key, result_saver)

    def handle_task_data(self, data):
        pass


class DomainEngine(ProcessEngine):

    def __init__(self, connection, queue, exchange, routing_key, result_saver):
        super().__init__(connection, queue, exchange, routing_key, result_saver)

    def handle_task_data(self, data):
        pass


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
exchange = kombu.Exchange(name="test_exchange",
                          type="direct",
                          durable=True)

# 创建一个队列对象，指定名称、交换机和路由键等参数，并绑定到交换机上
queue = kombu.Queue(name="test_queue",
                    exchange=exchange,
                    routing_key="test_key")
queue.bind(connection)


# 在创建引擎框架实例时，传入一个结果保存对象作为参数
result_saver = MySQLResultSaver()  # 创建一个MySQL结果保存对象
domain_engine = DomainEngine(connection, queue, exchange, "test_key", result_saver)  # 将结果保存对象传入引擎框架实例



# 调用下游子类的网络连接失败重试的方法，确保连接成功
domain_engine.run()










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

