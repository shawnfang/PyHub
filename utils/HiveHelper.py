# -*- coding: utf-8 -*-
from pyhive import hive
#from impala.dbapi import connect
import configparser
import os
from utils import configHelper

class HiveClient:
    def __init__(self,database):
        """
        create connection to hive server2
        """
        self.config = configHelper.ConfigHelper("hive.ini","hiveclient")
        self.configTulpe = self.config.readconfig()
        self.conn = hive.connect(host=self.configTulpe[0][1],
                                port=self.configTulpe[1][1],
                                username=self.configTulpe[2][1],
                                password=self.configTulpe[3][1],
                                auth=self.configTulpe[4][1],
                                database=database)


    def query(self, sql):
        """
        query
        """
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def insert(self, sql):
        """
        insert action
        """
        with self.conn.cursor() as cursor:
            try:
                cursor.execute(sql)
                self.conn.commit()
            except:
                self.conn.rollback()

    def close(self):
        """
        close connection
        """
        self.conn.close()


if __name__ == '__main__':
    conn = HiveClient("gmall")
    a = conn.query("SELECT * from stg_sale_account where id=3110 and dt='2019-03-02'")
    print(a)