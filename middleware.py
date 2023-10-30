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
