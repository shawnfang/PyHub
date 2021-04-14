# -*- coding: utf-8 -*-
import os
import configparser


class ConfigHelper:
    def __init__(self,filename=None,sections=None):
        # 路径处理
        try:
            self.curpath = os.path.dirname('../config/')
            self.cfgpath = os.path.join(self.curpath, filename)
        except Exception as e:
            print("路径不能为空".format(e))
        # 创建管理对象
        self.conf = configparser.ConfigParser()
        self.section = sections

    def readconfig(self):
        try:
            self.conf.read(self.cfgpath, encoding="utf-8")
            self.items = self.conf.items(self.section)
            return self.items
        except Exception as e:
            print("节点名称不能为空".format(e))
    def wirteconfig(self):
        pass

if __name__ == '__main__':
    c = ConfigHelper("hive.ini","hiveclient")
    print(c.readconfig())

