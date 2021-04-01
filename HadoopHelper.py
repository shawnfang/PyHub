# -*- coding: utf-8 -*-
import gzip
from hdfs.client import Client
from hdfs3 import HDFileSystem
import pandas as pd
import pyhdfs
from fastparquet import ParquetFile

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
print("读取长度")
print("~~~~~~~~~~~")

host = "nameservice1"
conf = {"dfs.nameservices": "nameservice1",
        "dfs.ha.namenodes.nameservice1": "namenode101",
        "dfs.namenode.rpc-address.nameservice1.namenode101": "hadoop101:8020",
        "dfs.namenode.http-address.nameservice1.namenode101": "hadoop101:9870",
        "hadoop.security.authentication": "kerberos"
}

hdfs = HDFileSystem(host='192.168.4.71',port=8020)
sc = hdfs.open("/warehouse/gmall/dwt/dwt_category_topic/000000_0")
pf = ParquetFile("test",open_with=sc)
df = pf.to_pandas()
print(df)
