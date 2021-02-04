#coding=utf-8
#!/usr/bin/python
import json

import pymysql


class MYSQLHelper:
    """
    对pymysql的简单封装
    """
    def __init__(self,host,port,user,pwd,db):
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.db = db

    def __GetConnect(self):
        """
        得到连接信息
        返回: conn.cursor()
        """
        if not self.db:
            raise(NameError,"没有设置数据库信息")
        self.conn = pymysql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="utf8")
        cur = self.conn.cursor()
        if not cur:
            raise(NameError,"连接数据库失败")
        else:
            return cur

    def get_index_dict(self,cursor):
        """
        获取数据库对应表中的字段名
        """
        index_dict = dict()
        index = 0
        for desc in cursor.description:
            index_dict[desc[0]] = index
            index = index + 1
        return index_dict

    def ExecQuery(self,sql):
        """
        执行查询语句
        返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段

        调用示例：
                ms = MYSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        cur = self.__GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()
        index_dict = self.get_index_dict(cur)
        res = []
        for datai in resList:
            resi = dict()
            for indexi in index_dict:
                resi[indexi] = datai[index_dict[indexi]]
            res.append(resi)
        return res
        '''
        查询完毕后必须关闭连接
        '''
        self.conn.close()
        return resList

    def ExecNonQuery(self,sql):
        """
        执行非查询语句

        调用示例：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        try:
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
            return 1
        except:
            return 0




def sqlrun(env,db,sql):
    sqlenv = ''
    if isinstance(env,str) and env[0]=='{':
        sqlenv=json.loads(env)
        host = sqlenv["host"]
        port = sqlenv["port"]
        user = sqlenv["user"]
        pwd = sqlenv["pwd"]
    else:
        host = env["host"]
        port = env["port"]
        user = env["user"]
        pwd = env["pwd"]
    db = db
    mysqlHelper = MYSQLHelper(host=host,port=port,user=user,pwd=pwd,db=db)
    if "insert" in sql or "INSERT" in sql:
        insertStatus= mysqlHelper.ExecNonQuery(sql)
        return insertStatus
    if  "update" in sql or "UPDATE" in sql:
        updateStatus = mysqlHelper.ExecNonQuery(sql)
        return updateStatus
    elif "select" or "SELECT" in sql:
        resList = mysqlHelper.ExecQuery(sql)
        return resList
    else:
        raise(NameError,"sql语句有误")


if __name__ == '__main__':
    service = '{"host":"192.168.2.10","port":"3306","user":"root","pwd":"123456"}'
    sql = "SELECT * from book_info"
    sql2 = "INSERT INTO `book`.`book_info`( `book_name`, `book_sort_id`, `book_author`, `book_price`, `book_type`, `book_publish`, `book_sum`, `book_mark`, `update_date`) VALUES ( 't5534', 1, 'fangxiao1', 101.00, 'kexue1', 'jixie1', '100', '0', '2020-09-28 07:24:15');"
    sql3 = "UPDATE `book`.`book_info` SET `book_publish` = 'ji333354654654xie1', `book_sum` = '10100', `book_mark` = '0', `update_date` = '2020-09-28 07:24:15' WHERE `book_id` = 14";
    print(sqlrun(service,"book",sql))
    print(sqlrun(service,"book", sql2))
    print(sqlrun(service,"book",sql3))