# -*- coding: utf-8 -*-
import gzip
from hdfs.client import Client
from hdfs3 import HDFileSystem
import pandas as pd
import pyhdfs
from fastparquet import ParquetFile
import petl as etl
import pymysql

def testhadoop():
    client = Client("http://hadoop101:9870/")
    print("hdfs中的目录:")
    print(client.list(hdfs_path="/",status=True))
    print("hdfs中的数据库:")
    print(client.list(hdfs_path="/warehouse/gmall",status=True))
    print("hdfs中目录下的文件数量:")
    print(client.checksum(hdfs_path="/warehouse/gmall/stg/stg_category/dt=2019-03-03/part-m-00000.lzo"))
    print("查看文件/目录状态")
    print(client.status(hdfs_path="/warehouse/gmall/stg/stg_category/dt=2019-03-03/part-m-00000.lzo",strict=True))
    print("列出文件目录或者详情")
    print(client.content(hdfs_path="/warehouse/gmall/stg/stg_category/dt=2019-03-03/part-m-00000.lzo",strict=True))


def testetl():
    connection = pymysql.connect(host="10.100.1.111", user="readonly",password="!QAZxsw2",database="sale",charset="utf8")
    table = etl.fromdb(connection, 'SELECT * FROM company')
    etl.fromdataframe(table)

if __name__ == '__main__':
    print(testetl())
