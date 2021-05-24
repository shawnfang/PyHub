#coding=utf-8
#!/usr/bin/python
import os
import requests

def monitorMen(dingToken=None,sendContent=None):
    # 查看内存情况
    val = os.popen("free")
    # 读取内存信息
    valList = val.readlines()
    # 以空格作为分隔符，把读取的信息转成一个列表
    valListSplit = valList[1].split(" ")
    # 去掉列表中的空格
    stripSpaceList = [x.strip() for x in valListSplit if x.strip() != '']
    # 取内存总量
    memTotal = stripSpaceList[1]
    # 取已使用内存量
    memUsed = stripSpaceList[2]
    print(stripSpaceList)
    # 计算已使用百分比
    memUsedPer = "%.2f%%" % (float(memUsed)/float(memTotal)*100)
    print(memUsedPer)
    # 转换为int类型，提供一下判断语句使用
    intMemUsePer = int(float(memUsedPer.strip("%")))
    print(intMemUsePer)
    if intMemUsePer > 90:
        # 接入钉钉
        DingHelp(dingToken,sendContent)
        os.popen("sh /usr/local/erp/startUserMgt.sh")
    else:
        DingHelp(dingToken, "当前系统正常，内存占比为%s" % memUsedPer)


def DingHelp(token,content=None,atMobiles=None,isAtAll=False):
    msg = {"msgtype": "text","text": {"content": "来自自定义机器人的默认消息"},"at": {"atMobiles": [], "isAtAll": False}}
    if atMobiles is not None and isinstance(atMobiles,str) :
        msg["at"]["atMobiles"].append(str(atMobiles))
    if atMobiles is not None and isinstance(atMobiles,list) :
        msg["at"]["atMobiles"]=atMobiles
    if isAtAll is not False:
        msg["at"]["isAtAll"] = True
    if content is not None:
        msg["text"]["content"] = content
    header = {'Content-Type': 'application/json'}
    rest = requests.session()
    rest.headers.update(header)
    params ={}
    params['access_token'] = token
    #dict1 = bodymsg
    res = rest.post('https://oapi.dingtalk.com/robot/send',params=params,json=msg,verify=False).text
    return res

if __name__ == '__main__':
    monitorMen("11bc7dbf8a74892e267193d649b803418919b69b3372ef22ae5573f0b12fed28","内存异常，重启用户系统")
