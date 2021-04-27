from utils import SqlalchemyEngine
from utils import HadoopHelper
import pandas as pd
import datacompy

class DataCheck:
    def __init__(self,mysqldb):
        self.db = SqlalchemyEngine.SqlalchemyClient(mysqldb)
        self.hadoop = HadoopHelper.HadoopClient()

    def check(self,sql,hdfsdataPath):
        self.sqldata = self.db.query(sql)
        self.downloadPath = self.hadoop.dataDownload(hdfsdataPath)
        self.hadoop.decompressLzo(self.downloadPath)
        self.hdfsdata = self.hadoop.readFile(self.downloadPath)
        print(self.sqldata)
        print(self.hdfsdata)
        self.a = self.sqldata.eq(self.hdfsdata)
        return self.a

if __name__ == '__main__':
    sql = "select * from sale_account"
    hdfsDataPath = "/warehouse/gmall/stg/stg_sale_account/dt=2019-03-01/part-m-00000.lzo"
    D = DataCheck("sale")
    print(D.check(sql,hdfsDataPath))