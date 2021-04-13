# coding: utf-8
import pika
import socket
import json

class rabbitMqHelp:
    def __init__(self,env,mq,payload):
        self.msg = []
        if isinstance(env,str) and env[0] == '{':
            self.env = json.loads(env)
        else:
            raise(NameError,"配置参数错误")
        self.mq = mq
        if isinstance(payload,list):
            self.payload = payload
        else:
            raise(NameError,"请求参数错误")

    def _connection(self):
        # 第一步，连接RabbitMQ服务器
        self.credentials = pika.PlainCredentials(self.env["user"],self.env["pwd"])
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(self.env['host'], self.env['port'], self.env['vhost'], self.credentials,
                                          socket_timeout=120))
        except pika.exceptions.ProbableAuthenticationError:
            self.msg.append('账号或者密码错误')
            return self.msg
        except socket.gaierror:
            self.msg.append('rabbitMQ环境url错误')
            return self.msg
        except pika.exceptions.AMQPConnectionError:
            self.msg.append('端口错误或者vhost名称错误')
            return self.msg
        except Exception as e:
            return e
        return self.connection


    def pushpayload(self):
        # 在连接上创建一个频道
        self.channel = self._connection().channel()
        # 第二步，声明一个队列，生产者和消费者都要声明一个相同的队列，用来防止万一某一方挂了，另一方能正常运行
        self.channel.queue_declare(queue=self.mq, durable=True)
        # 第三步，发送消息，routing_key填的是queue的名称，这里exchange填空字符串，使用了default exchange
        for i in range(len(self.payload)):
            print(json.dumps(self.payload[i]))
            if isinstance(self.payload[i],dict):
                self.channel.basic_publish(exchange='', routing_key=self.mq, body=json.dumps(self.payload[i]))
            else:
                self.msg.append('消息队列推送信息类型错误')
                return self.msg
        # 第四步，关闭连接
        self._connection().close()
        self.msg.append('success')
        return self.msg

if __name__ == '__main__':
    env = '''{"user":"guest","pwd":"guest","host":"192.168.3.185","port":"5672","vhost":"/"}'''
    mq = "EPMS_PUSH_CUSTOMMADE_TO_PRODUCT_QUEUE"
    payload = [{"mqType":None,"service":None,"sender":None,"recipient":None,"createTime":1613530579419,"topic":None,"body":"[{\"isCustomMade\":1,\"maxCustomMadeCycle\":2,\"minCustomMadeCycle\":1,\"sonSku\":\"7HH1107402\"}]","args":None,"save":True}]
    mq = rabbitMqHelp(env,mq,payload)
    mq.pushpayload()