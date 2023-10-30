

# 定义流程引擎框架类
from rabbit_middleware import RabbitMQMiddleware


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