#coding=utf-8
#!/usr/bin/python
import pandas as pd
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import  *
import prestodb
import configparser
import os
from utils import configHelper

class PrestoClient:
    def __init__(self,schema=None):
        self.config = configHelper.ConfigHelper("presto.ini", "presto")
        self.configTulpe = self.config.readconfig()
        # f-string的处理方式 presto://192.168.4.71:8881/hive/gmall
        self.conn = create_engine(f'presto://{self.configTulpe[0][1]}:{self.configTulpe[1][1]}/{self.configTulpe[2][1]}/{schema}')

    def query(self,sql):
        df = pd.read_sql(sql,self.conn)
        return df

if __name__ == '__main__':
    p = PrestoClient("gmall")
    df = p.query("select * from gmall.ods_user_info where dt='2019-03-01' limit 100")
    print(df)

