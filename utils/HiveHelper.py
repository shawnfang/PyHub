# -*- coding: utf-8 -*-
from pyhive import hive
#from impala.dbapi import connect

class HiveClient:
    def __init__(self, host='192.168.4.71', port=10000, username='root', password='!QAZxsw2', database='gmall', auth='LDAP'):
        """
        create connection to hive server2
        """
        self.conn = hive.connect(host=host,
                                port=port,
                                username=username,
                                password=password,
                                database=database,
                                auth=auth)


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
    conn = HiveClient()
    a = conn.query("SELECT * from stg_sale_account where id=3110 and dt='2019-03-02'")
    print(a)