#coding=utf-8
#!/usr/bin/python
from elasticsearch6 import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch import exceptions
import traceback
import datetime
import sys

class ESHelper:
    def __init__(self,esenv,index,type):
        self.__open = False
        if isinstance(esenv,str):
            self.__host = esenv.split(":")[0]
            self.__port = int(esenv.split(":")[1])
        self.index = index
        self.type = type
        self.es = Elasticsearch([{'host': self.__host, 'port': self.__port}],sniff_on_start=True,sniff_on_connection_fail=True,sniff_timeout=60)

    def search(self,condition):
        self.result = self.es.search(self.index,self.type,condition)
        print(self.result)
        if self.result['hits']['total'] == 0:
            raise(NameError,"查询的结果为空")
        else:
            return self.result['hits']['hits'][0]['_source']

    def update(self,index,type,id):
        return self.es.update(index,type,id)

if __name__ == '__main__':
    Es = ESHelper("192.168.4.71:9200","singleitemes","singleitemes")
    condition = {
            "query": {
                "bool": {
                  "must": [
                    {"match": {
                      "sonSku": "9SD800038-KH-XL"
                    }},
                    {
                      "term": {
                        "categoryId": 79
                      }
                    }
                    ]
                }
              }
        }
    print(type(condition))
    condition2 = {
  "query": {
    "bool": {
      "must_not": [
        {"match": {
          "devEmpId.keyword": 343
          }
        },
        {"match": {
          "assistantId.keyword": 7679
          }
        }
      ],
      "should": [
        {"match": {
          "length": 1000
        }},{
          "bool": {
            "must": [
              {"match": {
            "productWeight": 900
          }}
            ]
          }
        }
      ]
    }
  }
}

    result = Es.search(condition)
    print(type(result))
    print(result)