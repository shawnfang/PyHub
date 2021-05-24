from utils import SqlalchemyEngine
from utils import HadoopHelper
from utils import HiveHelper
import pandas as pd
import datacompy

class DataCheck:
    def __init__(self,mysqldb,hivedb):
        self.db = SqlalchemyEngine.SqlalchemyClient(mysqldb)
        self.hive = HiveHelper.HiveClient(hivedb)


    def checkHive(self,sql,hivesql):
        self.hivesql = self.hive.pdQuery(hivesql)
        self.mysql = self.db.query(sql)
        self.eq = datacompy.Compare(self.mysql,self.hivesql,join_columns=['id','account_number','seller_id'])
        print(self.eq.matches())
        print("#############################################################")
        print(self.eq.report())

    def showHive(self,hiveSql):
        self.hive = self.hive.pdQuery(hiveSql)
        print(self.hive)


if __name__ == '__main__':
    sql = "select id,account_number,seller_id from sale_account where create_date<='2021-05-07' or modified_date <= '2021-05-07'"
    hivesql = "SELECT id,account_number,seller_id from gmall.stg_sale_account where dt='2021-05-05'"
    D = DataCheck("sale","gmall")
    #D.checkHive(sql,hivesql)
    hivesql2 = "describe gmall.stg_sale_channel;"
    D.showHive(hivesql2)