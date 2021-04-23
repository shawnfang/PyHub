#coding=utf-8
#!/usr/bin/python
import requests
import json
import sys
sys.path.append(r'D:\github\PyHub')
import time
import datetime
from rediscluster import StrictRedisCluster
from pandas import *


class RequestClient(object):
    """
    为sosotest重新定义的requests类
    """

    def __init__(self):
        self.response_type = 0  # 若为1时 response的返回值的类型是str 及可以取出
        self.header_print = None  # 请求头
        self.url = None  # url
        self.data_print = None  # 参数
        self.response_text = None
        self.response_json = None  # 可以直接调用 返回值为json格式的时候
        self.session = requests.session()
        self.header = None
        self.api_root_url = None
        self.data = None  # 用来存储请求的参数
        self.news = ''

    def get(self, url, **kwargs):
        url = self.api_root_url + url
        self.url = url
        response = self.session.get(url, **kwargs)
        self.header_print = response.headers
        self.header_print = self.header_prints()
        try:
            self.response_type = 0
            self.response_json = json.dumps(response.json(), indent=4, ensure_ascii=False)
        except Exception as e:
            self.response_type = 1
            self.response_json = response.text
        return response

    def postParams(self, url, params=None, jsondata=None, **kwargs):
        url = self.api_root_url + url
        self.url = str(url)
        if params:
            self.data = params
            self.data_print = self.data
        elif json:
            self.data = jsondata
            self.data_print = self.data
        else:
            pass
        response = self.session.post(url, params=params, json=self.data_print, **kwargs)
        self.response_json = json.dumps(response.json(), indent=4, ensure_ascii=False)
        self.header_print = response.headers
        self.header_print = self.header_prints()
        try:
            self.response_type = 0
            self.response_json = response.json()
        except Exception as e:
            self.response_type = 1
            self.response_text = response.text
        return response

    def post(self, url, data=None, jsondata=None, **kwargs):
        url = self.api_root_url + url
        self.url = str(url)
        if data:
            self.data = data
            self.data_print = json.dumps(data, indent=4, ensure_ascii=False)
        elif json:
            self.data = jsondata
            self.data_print = json.dumps(data, indent=4, ensure_ascii=False)
        else:
            pass
        response = self.session.post(url, data, jsondata, **kwargs)
        self.response_json = json.dumps(response.json(), indent=4, ensure_ascii=False)
        self.header_print = response.headers
        self.header_print = self.header_prints()
        try:
            self.response_type = 0
            self.response_json = response.json()
        except Exception as e:
            self.response_type = 1
            self.response_text = response.text
        return response

    def options(self, url, **kwargs):
        url = self.api_root_url + url
        self.url = str(url)
        return self.session.options(url, **kwargs)

    def head(self, url, **kwargs):
        url = self.api_root_url + url
        self.url = str(url)
        return self.session.head(url, **kwargs)

    def put(self, url, jsondata=None, data=None, **kwargs):
        url = self.api_root_url + url
        self.url = str(url)
        if json:
            self.data = jsondata
            self.data_print = "请求参数" + str(json)
        else:
            self.data = data
            self.data_print = json.dumps(data, indent=4, ensure_ascii=False)
        response = self.session.put(url, json=jsondata, **kwargs)
        self.response_json = json.dumps(response.json(), indent=4, ensure_ascii=False)
        self.header_print = response.headers
        self.header_print = self.header_prints()
        try:
            self.response_type = 0
            self.response_json = response.json()
        except Exception as e:
            self.response_type = 1
            self.response_text = response.text
        return response

    def patch(self, url, data=None, **kwargs):
        url = self.api_root_url + url
        self.url = str(url)
        if data:
            self.data = data
            self.data_print = json.dumps(data, indent=4, ensure_ascii=False)
        response = self.session.patch(url, data, **kwargs)
        self.response_json = json.dumps(response.json(), indent=4, ensure_ascii=False)
        return response

    def delete(self, url, **kwargs):
        url = self.api_root_url + url
        self.url = str(url)
        response = self.session.delete(url, **kwargs)
        self.response_json = json.dumps(response.json(), indent=4, ensure_ascii=False)
        self.header_print = response.headers
        self.header_print = self.header_prints()
        try:
            self.response_type = 0
            self.response_json = response.json()
        except Exception as e:
            self.response_type = 1
            self.response_text = response.text

    def header_prints(self):
        self.header_printss = ''
        for key, value in dict(self.header_print).items():
            self.header_printss += key + ":" + value + '<br>'
        return self.header_printss

if __name__ == '__main__':

    url = 'https://oapi.dingtalk.com'
    header = {'Content-Type': 'application/json'}
    rest = RequestClient()
    rest.api_root_url ='https://oapi.dingtalk.com'
    rest.session.headers.update(header)
    params ={'access_token':'11bc7dbf8a74892e267193d649b803418919b69b3372ef22ae5573f0b12fed28'}
    dict1 = {"msgtype": "text","text": {"content": "我就是我, @15986808594 是不一样的烟火" },"at": {"atMobiles": ["15986808594" ], "isAtAll": False}}
    print(rest.api_root_url)
    res = rest.post('/robot/send',params=params,jsondata=dict1,verify=False)
    print(res.text)





