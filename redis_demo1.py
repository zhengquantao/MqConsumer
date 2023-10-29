import redis
import threading

class RedisConsumer:
    def __init__(self, host, port, db):
        self.redis = redis.Redis(host=host, port=port, db=db)

    def consume(self, key):
        while True:
            # 从Redis队列中消费数据
            data = self.redis.lpop(key)
            if data is not None:
                print('Received data: {}'.format(data))

                # 处理数据...
                result = self.process_data(data)

                # 将消费完的数据重新投递至Redis
                self.redis.rpush(key, result)

    def process_data(self, data):
        # 这是一个抽象方法，需要子类根据不同的业务逻辑来实现
        raise NotImplementedError

class MyRedisConsumer(RedisConsumer):
    def process_data(self, data):
        # 这里是处理数据的代码，具体实现取决于你的业务逻辑
        return data

# 使用示例
consumer = MyRedisConsumer('localhost', 6379, 0)

# 创建线程来运行消费者
thread = threading.Thread(target=consumer.consume, args=('my_queue',))
thread.start()
