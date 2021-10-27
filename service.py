#coding:utf-8

from fastapi import FastAPI, Form
from MySQLhelper import MySQLhelper
from starlette.responses import RedirectResponse
import uvicorn
import requests
import runPboc_online
import pymysql
import datetime
import hashlib
import json
import configparser
import uvicorn.logging
import uvicorn.loops
import uvicorn.loops.auto
import uvicorn.protocols
import uvicorn.protocols.websockets
import uvicorn.protocols.websockets.auto
import uvicorn.protocols.http
import uvicorn.protocols.http.auto
import uvicorn.lifespan
import uvicorn.lifespan.on
import uvicorn.lifespan.off
from starlette.middleware.cors import CORSMiddleware
from pydantic import BaseModel




app = FastAPI()
origins = [
    "http://172.18.2.216:8081",
    "http://127.0.0.1:8080",
    "http://127.0.0.1"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

class DBUtil():
    def __init__(self):
        conf = configparser.ConfigParser()
        conf.read("config.ini")
        self.host = conf.get("DataBase", "host")
        self.port = int(conf.get("DataBase", "port"))
        self.database = conf.get("DataBase", "database")
        self.user = conf.get("DataBase", "user")
        self.password = conf.get("DataBase", "password")
        self.conn = None
        self.cur = None

    def ConnectTODB(self):
        flag = False
        try:
            self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                        database=self.database)
            self.cur = self.conn.cursor()
            flag = True
        except Exception as e:
            print(e)
            return flag
        return flag

    def disConnDB(self):
        if self.cur != None:
            self.cur.close()
        if self.conn != None:
            self.conn.close()

    # 人行数据取数
    def handle_rhData(self, loanNo):

        # 连接数据库
        sql = "select applyInfo, rhInfo from model_data where loanNo = %s" % (loanNo)

        try:
            self.cur.execute(sql)
            results = self.cur.fetchone()
            # loanNo 如果不存在
            if results is not None:

                return results
            else:
                res = 'loanNo不存在'
                return res
        except Exception as e:
            print(e)

    # 百行数据取数
    def handle_bhData(self, loanNo):

        # 连接数据库
        sql = "select applyInfo, bhInfo from model_data where loanNo = %s" % (loanNo)

        try:
            self.cur.execute(sql)
            results = self.cur.fetchone()
            # loanNo 如果不存在
            if results is not None:

                return results
            else:
                res = 'loanNo不存在'
                return res
        except Exception as e:
            print(e)

    # 华道数据取数
    def handle_huadaoData(self, loanNo):

        # 连接数据库
        sql = "select applyInfo, huadaoInfo from model_data where loanNo = %s" % (loanNo)

        try:
            self.cur.execute(sql)
            results = self.cur.fetchone()
            # loanNo 如果不存在
            if results is not None:

                return results
            else:
                res = 'loanNo不存在'
                return res
        except Exception as e:
            print(e)

    # 京东数据取数
    def handle_jingdongData(self, loanNo):

        # 连接数据库
        sql = "select applyInfo, jingdongInfo from model_data where loanNo = %s" % (loanNo)

        try:
            self.cur.execute(sql)
            results = self.cur.fetchone()
            # loanNo 如果不存在
            if results is not None:

                return results
            else:
                res = 'loanNo不存在'
                return res
        except Exception as e:
            print(e)

    # 新颜数据取数
    def handle_xinyanData(self, loanNo):

        # 连接数据库
        sql = "select applyInfo, xinyanInfo from model_data where loanNo = %s" % (loanNo)

        try:
            self.cur.execute(sql)
            results = self.cur.fetchone()
            # loanNo 如果不存在
            if results is not None:

                return results
            else:
                res = 'loanNo不存在'
                return res
        except Exception as e:
            print(e)

    # 百融数据取数
    def handle_bairongData(self, loanNo):

        # 连接数据库
        sql = "select applyInfo, bairongInfo from model_data where loanNo = %s" % (loanNo)

        try:
            self.cur.execute(sql)
            results = self.cur.fetchone()
            # loanNo 如果不存在
            if results is not None:

                return results
            else:
                res = 'loanNo不存在'
                return res
        except Exception as e:
            print(e)


# 模型
class Model(BaseModel):
    # 订单编号
    loanNo: str = ""


class Item(BaseModel):
    # applyInfo: dict = None
    #
    # rhInfo: dict = None
    #
    # bhInfo: dict = None
    #
    # huadaoInfo: dict = None
    #
    # jingdongInfo: dict = None
    #
    # xinyanInfo: dict = None
    #
    # bairongInfo: dict = None
    #
    # # mongoInfo: dict = None
    #
    # youfenInfo: dict = None
    #
    # bhphInfo: dict = None
    #
    # shebaoInfo: dict = None
    #
    # ruleInfo: dict = None
    #
    # riskGoRank: dict = None
    #
    # channel: int
    requestBody: str

# 百融接口处理逻辑
@app.post("/model/bairongData")
async def bairongData(model: Model):

    if model.loanNo is None or model.loanNo == "":
        results = {"code": 1, "data": "loanNo为空", "msg": "失败"}
        return results
    else:

        db = DBUtil()
        if db.ConnectTODB():

            data = db.handle_bairongData(model.loanNo)
            if data == 'loanNo不存在':
                return {"code": 1, "data": {"loanNo": model.loanNo}, "msg": "订单号不存在"}

            else:
                # 对数据是否存在做出判断
                if data[0] is None:
                    applyInfo = '{}'
                else:
                    applyInfo = data[0]

                if data[1] is None:
                    bairongInfo = '{}'
                else:
                    bairongInfo = data[1]
                response = runPboc_online.bairongDataOnline(applyInfo=json.loads(applyInfo), bairongInfo=json.loads(bairongInfo))

                return response
        else:
            return {"code": 1, "data": {"loanNo": model.loanNo}, "msg": "数据库连接失败"}


# 新颜接口处理逻辑
@app.post("/model/xinyanData")
async def xinyanData(model: Model):

    if model.loanNo is None or model.loanNo == "":
        results = {"code": 1, "data": "loanNo为空", "msg": "失败"}
        return results
    else:

        db = DBUtil()
        if db.ConnectTODB():

            data = db.handle_xinyanData(model.loanNo)
            if data == 'loanNo不存在':
                return {"code": 1, "data": {"loanNo": model.loanNo}, "msg": "订单号不存在"}

            else:
                # 对数据是否存在做出判断
                if data[0] is None:
                    applyInfo = '{}'
                else:
                    applyInfo = data[0]

                if data[1] is None:
                    xinyanInfo = '{}'
                else:
                    xinyanInfo = data[1]

                response = runPboc_online.xinyanDataOnline(applyInfo=json.loads(applyInfo), xinyanInfo=json.loads(xinyanInfo))

                return response
        else:
            return {"code": 1, "data": {"loanNo": model.loanNo}, "msg": "数据库连接失败"}



# 京东接口处理逻辑
@app.post("/model/jingdongData")
async def jingdongData(model: Model):

    if model.loanNo is None or model.loanNo == "":
        results = {"code": 1, "data": "loanNo为空", "msg": "失败"}
        return results
    else:

        db = DBUtil()
        if db.ConnectTODB():

            data = db.handle_jingdongData(model.loanNo)
            if data == 'loanNo不存在':
                return {"code": 1, "data": {"loanNo": model.loanNo}, "msg": "订单号不存在"}

            else:
                # 对数据是否存在做出判断
                if data[0] is None:
                    applyInfo = '{}'
                else:
                    applyInfo = data[0]

                if data[1] is None:
                    jingdongInfo = '{}'
                else:
                    jingdongInfo = data[1]

                response = runPboc_online.jingdongDataOnline(applyInfo=json.loads(applyInfo), jingdongInfo=json.loads(jingdongInfo))

                return response
        else:
            return {"code": 1, "data": {"loanNo": model.loanNo}, "msg": "数据库连接失败"}

# 华道接口处理逻辑
@app.post("/model/huadaoData")
async def huadaoData(model: Model):

    if model.loanNo is None or model.loanNo == "":
        results = {"code": 1, "data": "loanNo为空", "msg": "失败"}
        return results
    else:

        db = DBUtil()
        if db.ConnectTODB():

            data = db.handle_huadaoData(model.loanNo)
            if data == 'loanNo不存在':
                return {"code": 1, "data": {"loanNo": model.loanNo}, "msg": "订单号不存在"}

            else:
                # 对数据是否存在做出判断
                if data[0] is None:
                    applyInfo = '{}'
                else:
                    applyInfo = data[0]

                if data[1] is None:
                    huadaoInfo = '{}'
                else:
                    huadaoInfo = data[1]

                response = runPboc_online.huadaoDataOnline(applyInfo=json.loads(applyInfo), huadaoInfo=json.loads(huadaoInfo))

                return response
        else:
            return {"code": 1, "data": {"loanNo": model.loanNo}, "msg": "数据库连接失败"}


# 人行接口处理逻辑
@app.post("/model/rhData")
async def rhData(model: Model):

    if model.loanNo is None or model.loanNo == "":
        results = {"code": 1, "data": "loanNo为空", "msg": "失败"}
        return results
    else:

        db = DBUtil()
        if db.ConnectTODB():

            # print(model.loanNo)
            data = db.handle_rhData(model.loanNo)
            if data == 'loanNo不存在':
                return {"code": 1, "data": {"loanNo": model.loanNo}, "msg": "订单号不存在"}

            else:
                # 对数据是否存在做出判断
                if data[0] is None:
                    applyInfo = '{}'
                else:
                    applyInfo = data[0]

                if data[1] is None:
                    rhInfo = '{}'
                else:
                    rhInfo = data[1]
                # print(rhInfo)
                # print(type(rhInfo))
                # print(json.loads(rhInfo))
                # print(type(json.loads(rhInfo)))
                response = runPboc_online.rhDataOnline(applyInfo=json.loads(applyInfo), rhInfo=json.loads(rhInfo))

                return response
        else:
            return {"code": 1, "data": {"loanNo": model.loanNo}, "msg": "数据库连接失败"}



# 百行接口
@app.post("/model/bhData")
async def bhData(model: Model):

    if model.loanNo is None or model.loanNo == "":
        results = {"code": 1, "data": "loanNo为空", "msg": "失败"}
        return results
    else:

        db = DBUtil()
        if db.ConnectTODB():

            # print(model.loanNo)
            data = db.handle_bhData(model.loanNo)
            if data == 'loanNo不存在':
                return {"code": 1, "data": {"loanNo": model.loanNo}, "msg": "订单号不存在"}

            else:
                # 对数据是否存在做出判断
                if data[0] is None:
                    applyInfo = '{}'
                else:
                    applyInfo = data[0]

                if data[1] is None:
                    bhInfo = '{}'
                else:
                    bhInfo = data[1]

                response = runPboc_online.bhDataOnline(applyInfo=json.loads(applyInfo), bhInfo=json.loads(bhInfo))

                return response
        else:
            return {"code": 1, "data": {"loanNo": model.loanNo}, "msg": "数据库连接失败"}


# 人行接口处理逻辑
@app.post("/model/yzj_strategy_D")
async def rhData_new(requestBody: str = Form(...)):
        # print(applyInfo)
        # print(rhInfo)

        # print(type(rhInfo))
        # print(json.loads(rhInfo))
        # print(type(json.loads(rhInfo)))
        # if item.applyInfo:
        #     item.applyInfo = '{}'
        # elif item.rhInfo:
        #     item.rhInfo = '{}'

        request_body = json.loads(requestBody)
        channel = request_body['channel']

        response = runPboc_online.runMain(request_body=request_body, channel=channel)
        # response = runPboc_online.rhDataOnline(applyInfo=json.loads(item.applyInfo), rhInfo=json.loads(item.rhInfo), channel=item.channel)
        # print(item.requestBody, item.applyInfo)
        print(type(requestBody))
        print(response)

        # print(response)
        return json.dumps(response)



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5001)

