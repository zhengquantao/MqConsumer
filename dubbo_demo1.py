from pydubbo.client import DubboClient, DubboClientError


class DubboConsumer:
    def __init__(self, host, port):
        self.client = DubboClient(host, port)

    def consume(self, service, method, args):
        try:
            # 调用Dubbo服务
            result = self.client.invoke(service, method, args)
            print('Received result: {}'.format(result))

            # 将消费完的数据重新投递至Dubbo
            self.client.invoke(service, method, result)

        except DubboClientError as e:
            print("Error:", e)

# 使用示例
consumer = DubboConsumer('localhost', 12345)
consumer.consume('com.example.MyService', 'myMethod', ['myArg'])