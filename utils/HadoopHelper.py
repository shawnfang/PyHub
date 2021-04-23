#coding=utf-8
#!/usr/bin/python
from hdfs.client import Client
import os
import lzo
import zlib
from struct import pack, unpack
from utils import configHelper
import traceback
import logging
import datetime
#import lzo_indexer
import pandas as pd

class HadoopClient:
    def __init__(self):
        self.config = configHelper.ConfigHelper("hadoop.ini", "hdfs")
        self.configTulpe = self.config.readconfig()
        self.client = Client(f'http://{self.configTulpe[0][1]}:{self.configTulpe[1][1]}/')
        self.downloadDataPath = configHelper.ConfigHelper("fileconfig.ini","tmp")
        self.configPath = self.downloadDataPath.readconfig()
        self.savePath = None
        self.dataDownloadDir = None
        self.dataFilePath = None

    # 下载hdfs的数据原件
    # 返回文件的路径
    def dataDownload(self,dataPath):
        self.dataDownloadDir = dataPath.split("/")[-2]
        self.savePath = self.configPath[0][1]+"/"+self.dataDownloadDir
        os.mkdir(self.savePath)
        rst = self.client.download(dataPath, self.savePath)
        if isinstance(rst,str):
            return rst
        else:
            return 0

    # 解压缩 lzo文件
    # 返回解压文件的路径
    def decompressLzo(self,dataPath):
        self.result = os.system("lzop -dv %s" % dataPath)
        if self.result == 0:
            self.dataFilePath = dataPath.split(".")[0]
            return self.dataFilePath
        else:
            return 0

    def readFile(self,filePath):
        pd.set_option('display.max_columns',None)
        return pd.read_table(filePath,sep='\t',header=None,index_col=None,
                               dtype=None, encoding='utf-8', engine=None, nrows=None)


if __name__ == '__main__':
    H = HadoopClient()
    #path = H.dataDownload("/warehouse/gmall/stg/stg_category/dt=2019-03-06/part-m-00000.lzo")
    #print(H.decompressLzo(path))
    print(H.readFile("/usr/local/WarehouseData/tmpdata/dt=2019-03-06/part-m-00000"))
