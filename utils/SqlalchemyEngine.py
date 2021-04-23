#coding=utf-8
#!/usr/bin/python
import pandas as pd
from sqlalchemy import create_engine
from utils import configHelper

class SqlalchemyClient:
    def __init__(self,database):
        self.config = configHelper.ConfigHelper("mysql.ini", "mysql-online1")
        self.configTulpe = self.config.readconfig()
        # f-string的处理方式 presto://192.168.4.71:8881/hive/gmall
        self.conn = create_engine(f'mysql+pymysql://{self.configTulpe[2][1]}:{self.configTulpe[3][1]}@{self.configTulpe[0][1]}:{self.configTulpe[1][1]}/{database}')

    def query(self,sql):
        df_read = pd.read_sql(sql,self.conn)
        print(df_read)

if __name__ == '__main__':
    s = SqlalchemyClient("sale")
    sql = "select * from company"
    s.query(sql)