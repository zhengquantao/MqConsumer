
import time
import traceback

# 定义一个RabbitMQ中间件类，继承自中间件类
from kombu import Consumer

from middleware import Middleware


class RabbitMQMiddleware(Middleware):
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
        with self.connection.Producer() as producer:
            # # 确保队列已经存在
            # queue = Queue(name="test_key", exchange=exchange, routing_key="test_key")
            # queue.maybe_bind(conn)
            # queue.declare()

            producer.publish(result,
                             exchange=self.exchange,
                             routing_key=self.routing_key,
                             serializer="json",
                             retry=True)

    def run(self):
        with Consumer(self.connection, queues=self.queue, callbacks=[self.on_message],
                      accept=["text/plain", "application/json"]):
            while True:
                try:
                    self.connection.drain_events(timeout=600)  # 设置超时时间
                except self.connection.connection_errors:
                    print(f"Connection error: {traceback.format_exc()}")
                    # self.connection.ensure(self, self.run)
                    time.sleep(2)  # 如果连接失败，等待一段时间再重试
