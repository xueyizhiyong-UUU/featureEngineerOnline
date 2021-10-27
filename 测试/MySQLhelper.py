import time
import pymysql
import pandas as pd
import threading
from dbutils.pooled_db import PooledDB, SharedDBConnection

class MySQLhelper(object):
    def __init__(self, host, port, dbuser, password, database):
        self.pool = PooledDB(
            creator=pymysql,  # 使用链接数据库的模块
            maxconnections=6,  # 连接池允许的最大连接数，0和None表示不限制连接数
            mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
            maxcached=5,  # 链接池中最多闲置的链接，0和None不限制
            maxshared=3,
            # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
            blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
            maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
            setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
            ping=0,
            # ping MySQL服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is created, 4 = when a query is executed, 7 = always
            host=host,
            port=int(port),
            user=dbuser,
            password=password,
            database=database,
            charset='utf8'
        )



    def create_conn_cursor(self):
        conn = self.pool.connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        return conn,cursor

    def fetch_all(self, sql):
        self.conn, self.cursor = self.create_conn_cursor()
        self.cursor.execute(sql)
        rows = pd.read_sql(sql, self.conn)
        # self.cursor.close()
        # conn.close()
        return rows


    def insert_one(self,sql,args):
        conn,cursor = self.create_conn_cursor()
        res = cursor.execute(sql,args)
        conn.commit()
        print(res)
        conn.close()
        return res

    def update(self,sql,args):
        conn,cursor = self.create_conn_cursor()
        res = cursor.execute(sql,args)
        conn.commit()
        print(res)
        conn.close()
        return res


def pool_sql(sql, conn):
    if conn=='ods_credit_1':
        sqlhelper_ods_credit = MySQLhelper("121.199.3.118", 3306, "root", "Cq9NYpAc0ydxOj22N2NB", "ods_credit")
        return sqlhelper_ods_credit

    if conn=='ods_credit':
        sqlhelper_ods_credit = MySQLhelper("121.199.3.118", 3306, "root", "Cq9NYpAc0ydxOj22N2NB", "ods_credit")
        return sqlhelper_ods_credit.fetch_all(sql)
    if conn=='ods_tps':
        sqlhelper_ods_tps = MySQLhelper("121.199.3.118", 3306, "root", "Cq9NYpAc0ydxOj22N2NB", "ods_tps")
        return sqlhelper_ods_tps.fetch_all(sql)

    if conn == 'tps':
        sqlhelper_risk_tps = MySQLhelper("rm-bp14gc0109t445oe2mo.mysql.rds.aliyuncs.com", 3306, "riskreader",
                                         "843695abca!", "tps")
        return sqlhelper_risk_tps.fetch_all(sql)
    if conn == 'credit':
        sqlhelper_credit = MySQLhelper("rm-bp13n5lha3o0057hm0o.mysql.rds.aliyuncs.com", 3306, "qnreadonly", "oxKbx#22",
                                       "credit")
        return sqlhelper_credit.fetch_all(sql)
    if conn=='alchemist':
        sqlhelper_alchemist = MySQLhelper("rm-bp14gc0109t445oe2mo.mysql.rds.aliyuncs.com", 3306, "riskreader", "843695abca!", "alchemist")
        return sqlhelper_alchemist.fetch_all(sql)
    
    if conn=='tps_test':
        sqlhelper_tps_test = MySQLhelper("122.227.186.130", 6033, "devops", "7FPduLdaSL5NOZvn", "tps")
        return sqlhelper_tps_test.fetch_all(sql)
    
    if conn=='risk_alchemist':
        sqlhelper_risk_alchemist = MySQLhelper("121.199.3.118", 3306, "root", "Cq9NYpAc0ydxOj22N2NB", "risk_alchemist")
        return sqlhelper_risk_alchemist.fetch_all(sql)
    if conn=='ods_alchemist':
        sqlhelper_ods_alchemist = MySQLhelper("121.199.3.118", 3306, "root", "Cq9NYpAc0ydxOj22N2NB", "ods_alchemist")
        return sqlhelper_ods_alchemist.fetch_all(sql)

    if conn=='ods_carloan':
        sqlhelper_ods_carloan = MySQLhelper("121.199.3.118", 3306, "root", "Cq9NYpAc0ydxOj22N2NB", "ods_carloan")
        return sqlhelper_ods_carloan.fetch_all(sql)
    if conn=='ods_base_api_server':
        sqlhelper_ods_base_api_server = MySQLhelper("121.199.3.118", 3306, "root", "Cq9NYpAc0ydxOj22N2NB", "ods_base_api_server")
        return sqlhelper_ods_base_api_server.fetch_all(sql)
    if conn=='ods_alchemist_temp':
        sqlhelper_ods_alchemist_temp = MySQLhelper("121.199.3.118", 3306, "root", "Cq9NYpAc0ydxOj22N2NB", "ods_alchemist_temp")
        return sqlhelper_ods_alchemist_temp.fetch_all(sql)
    if conn=='ods_alchemist_external':
        sqlhelper_ods_alchemist_external = MySQLhelper("121.199.3.118", 3306, "root", "Cq9NYpAc0ydxOj22N2NB", "ods_alchemist_external")
        return sqlhelper_ods_alchemist_external.fetch_all(sql)
    if conn=='risk_dw':
        sqlhelper_risk_dw = MySQLhelper("121.199.3.118", 3306, "root", "Cq9NYpAc0ydxOj22N2NB", "risk_dw")
        return sqlhelper_risk_dw.fetch_all(sql)
    if conn=='ods_ypzw':
        sqlhelper_ods_ypzw = MySQLhelper("121.199.3.118", 3306, "root", "Cq9NYpAc0ydxOj22N2NB", "ods_ypzw")
        return sqlhelper_ods_ypzw.fetch_all(sql)
    if conn=='ods_ypzl':
        sqlhelper_ods_ypzl = MySQLhelper("121.199.3.118", 3306, "root", "Cq9NYpAc0ydxOj22N2NB", "ods_ypzl")
        return sqlhelper_ods_ypzl.fetch_all(sql)
    if conn=='ods_ypz':
        sqlhelper_ods_ypz = MySQLhelper("121.199.3.118", 3306, "root", "Cq9NYpAc0ydxOj22N2NB", "ods_ypz")
        return sqlhelper_ods_ypz.fetch_all(sql)

    if conn=='ods_risk':
        sqlhelper_ods_risk = MySQLhelper("121.199.3.118", 3306, "root", "Cq9NYpAc0ydxOj22N2NB", "ods_risk")
        return sqlhelper_ods_risk.fetch_all(sql)
    if conn=='ods_oauth2':
        sqlhelper_ods_oauth2 = MySQLhelper("121.199.3.118", 3306, "root", "Cq9NYpAc0ydxOj22N2NB", "ods_oauth2")
        return sqlhelper_ods_oauth2.fetch_all(sql)
    
    if conn=='xinyan_pboc':
        sqlhelper_ods_oauth2 = MySQLhelper("rm-bp14gc0109t445oe2mo.mysql.rds.aliyuncs.com", 3306, "cdvariable", "Nz37eLafR", "cd_variable")
        return sqlhelper_ods_oauth2.fetch_all(sql)
    if conn=='local_phone':
        sqlhelper_ods_oauth2 = MySQLhelper("localhost", 3306, "root", "amtf", "phone_location")
        return sqlhelper_ods_oauth2.fetch_all(sql)
    
    
    
    
    
    
    
    
    
    
    

