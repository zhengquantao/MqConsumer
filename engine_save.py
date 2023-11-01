# 定义一个结果保存类，这是一个抽象类，定义了一个抽象方法save
class ResultSaver:
    def save(self, result):
        raise NotImplementedError


# 定义一个MySQL结果保存类，继承自结果保存类
class MySQLResultSaver(ResultSaver):
    def save(self, result):
        # 这里是保存到MySQL数据库的代码
        print(f"save into mysql db")
        pass


# 定义一个Redis结果保存类，继承自结果保存类
class RedisResultSaver(ResultSaver):
    def save(self, result):
        # 这里是保存到Redis数据库的代码
        print(f"save into redis db")
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
        print(f"save into mongo db")
        pass
