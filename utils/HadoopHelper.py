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
from datetime import datetime
import time
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
    def dataDownload(self,filePath):
        self.dataDownloadDir = filePath.split('/')[-2]
        self.savePath = '/'+self.configPath[0][1]+"/"+self.dataDownloadDir+'-'+str(time.time())
        os.mkdir(self.savePath)
        rst = self.client.download(filePath, self.savePath)
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
        self.newFilePath = filePath[:-4]
        pd.set_option('display.max_columns',None)
        return pd.read_table(self.newFilePath,sep='\t',header=None,index_col=None,
                               dtype=None, encoding='utf-8', engine=None, nrows=None,error_bad_lines=False)

    def fileList(self,hdfsPath):
        return self.client.list(hdfsPath,status=False)

    def fileDelete(self,hdfsPath):
        pass

if __name__ == '__main__':
    H = HadoopClient()
    path = H.dataDownload("/warehouse/gmall/stg/stg_sale_account/dt=2019-03-01/part-m-00000.lzo")
    print(path)
    print(H.decompressLzo(path))
    print(H.readFile(path))
    #print(H.fileList("/warehouse/gmall/stg/stg_category"))
