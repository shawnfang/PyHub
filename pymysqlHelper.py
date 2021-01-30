#coding=utf-8
#!/usr/bin/python

import pymysql


class MYSQLHelper:
    """
    对pymysql的简单封装
    """
    def __init__(self,host,user,pwd,db):
        self.host = host
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
        cur = self.__GetConnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()




def main():

    mysqlhelper = MYSQLHelper(host="localhost",user="root",pwd="123456",db="fxdatabase")
    resList = mysqlhelper.ExecQuery("SELECT * from article_info")
    print(resList[0])
    print(resList[1])
    #for inst in resList:
    #    print(inst)
    mysqlhelper.ExecNonQuery("INSERT INTO fxdatabase.course(coursename, course_level) VALUES ('test13333', 'high33332');")
    mysqlhelper.ExecNonQuery("UPDATE fxdatabase.course SET coursename = 'test1111', course_level = 'high222222' WHERE coursename = 'test1' AND course_level = 'high2' LIMIT 1;")
if __name__ == '__main__':
    main()