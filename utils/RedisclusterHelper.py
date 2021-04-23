#coding=utf-8
#!/usr/bin/python
from rediscluster import StrictRedisCluster

class RedisCluster:  # 连接redis集群
    def __init__(self,conn_list):
        self.host = ''
        self.port = ''
        self.node = {}
        self.conn_list = []
        if(isinstance(conn_list,str)):
            self.host = conn_list.split(":")[0]
            self.port = int(conn_list.split(":")[1])
        self.node['host'] = self.host
        self.node['port'] = self.port
        self.conn_list.append(self.node)  # 连接列表

    def connect(self):
        """
        连接redis集群
        :return: object
        """
        try:
            # 非密码连接redis集群
            # redisconn = StrictRedisCluster(startup_nodes=self.conn_list)
            # 使用密码连接redis集群
            redisconn = StrictRedisCluster(startup_nodes=self.conn_list, password='')
            return redisconn
        except Exception as e:
            print(e)
            print("错误,连接redis 集群失败")
            return False

    def operator_status(func):
        '''get operatoration status
        '''

        def gen_status(*args, **kwargs):
            error, result = None, None
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                result = str(e)
            return result

        return gen_status

    # 获取KEY值
    @operator_status
    def rget(self,key):
        return str(self.connect().get(key), encoding="utf-8")

    # 设置字符类型key
    @operator_status
    def rset(self,key,data):
        return self.connect().set(key,data)

    # 返回集合的所有成员
    @operator_status
    def rsmembers(self,gset):
        strset = set()
        for x in self.connect().smembers(gset):
            strset.add(str(x,encoding="utf-8"))
        return strset

    # 往集合中添加成员
    @operator_status
    def rsadd(self,keyset,valueset):
        return self.connect().sadd(keyset,valueset)

    # 往list中插入数据
    @operator_status
    def rlpush(self,data):
        return self.connect().lpush(data)

    # 通过索引，获取list中对应的值
    @operator_status
    def rlindex(self,listkey,index):
        return self.connect().lindex(listkey,index)


if __name__ == '__main__':
    redis_basis_conn = "192.168.3.6:7006"
    conn = RedisCluster(redis_basis_conn)
    print(conn.rget('PRODUCT_SINGLE_SON_SKU7HH102367-R-100'))
    print(conn.rset("fangxiao","1236"))
    print(conn.rsadd("test-fangxiao","fangxiao1"))
    print(conn.rsmembers("test-fangxiao"))