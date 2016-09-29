#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2016-09-27 14:20:35
# Project: bank

from pyspider.libs.base_handler import *
import datetime as dt 
import re
import traceback
from pymongo import MongoClient
import xml.etree.ElementTree as ET

#银行字典{URL的type值：银行名}
bankType_dict = {'1':u'中国银行','2':u'工商银行','3':u'农业银行','5':u'招商银行','6':u'交通银行','7':u'光大银行','8':u'浦发银行','9':u'兴业银行'}
#数据字典
data_str = '''
<config>
    <colname>
        <code>代码</code>
        <currency>币种</currency>
        <nowPrice>现价</nowPrice>
        <rose>涨幅</rose>
        <upDown>涨跌</upDown>
        <openQuotation>开盘</openQuotation>
        <high>最高</high>
        <low>最低</low>
        <amplitude>振幅</amplitude>
        <buy>买价</buy>
        <sell>卖价</sell>
        <time>时间</time>
        <date>日期</date>
    </colname>
    <bank>
        <BOC>中国银行</BOC>
        <ICBC>工商银行</ICBC>
        <ABC>农业银行</ABC>
        <CMB>招商银行</CMB>
        <BCM>交通银行</BCM>
        <CEB>光大银行</CEB>
        <SPDB>浦发银行</SPDB>
        <CIB>兴业银行</CIB>
    </bank>
</config>
'''        


class Handler(BaseHandler):
    crawl_config = {
    }

    @every(minutes=5)
    def on_start(self):
        
        now  = dt.datetime.now().strftime('%H%M%S')
        #待爬取url列表
        url_list = ['http://quote.forex.hexun.com/hqzx/restquote.aspx?type=%s&time=%s'%(types,now) for types in bankType_dict.keys()]
        self.crawl(url_list, callback=self.index_page)

    @config(age=10)
    def index_page(self, response):
        #数据处理
        now  = dt.datetime.now().strftime('%Y-%m-%d')
        xml = XML(data_str)
        colname_dict = xml.getcolname()
        re_colname_dict = {value:key for key, value in colname_dict.items()}
        colname = map(lambda x:re_colname_dict[x],[u'代码',u'币种',u'现价',u'涨幅',u'涨跌',u'开盘',u'最高',u'最低',u'振幅',u'买价',u'卖价',u'时间'])
        print colname
        text =  response.text
        text = re.findall('(\[\[.*\]\]);',text)
        if text !=[]:
            text = text[0]
            text  = eval(text) 
            print text
            try:
                type = re.findall('type=(\d)',response.url)[0]
                bank_dict = xml.getbank()                
                re_bank_dict = {value:key for key, value in bank_dict.items()}
                print re_bank_dict
                print bankType_dict[type]
                bank_name = re_bank_dict[bankType_dict[type]]
                print bank_name
                data = map(lambda x:dict(dict(zip(colname,x)),**{'bank':bank_name,'date':now}),text)
                print data
                #数据库操作
                db = mongodb()                
                if db.find('bank')==0 or len(db.find('bank',{'bank':bank_name,'date':{'$eq':now}}))==0:
                    db.insert('bank',data)
                db.close()
            except:
                traceback.print_exc()
        

class mongodb(object):
    """
    数据库操作
    """
    def __init__(self):
       """
       初始化数据库
       """
       self.connect()
        
    def connect(self):
       """
       连接数据库
       """
       try:
           client=MongoClient('localhost',27017)
           db=client.bank_db
           self.__conn=db
       except:
           traceback.print_exc()
           self.__conn=None
                
    def insert(self,collection_name,list_data):
        """
        向collection插入数据操作
        collection_name：数据库文档名称
        list_data：插入的数据，格式为列表        
        示例：insert('test_collection',[{'a':'b'}])
        """
        collection=self.__conn[collection_name]
        try:
           collection.insert_many(list_data)
        except:
           traceback.print_exc()
        
            
    def delete(self,collection_name,dict_data={}):
        """
        向已有的collection删除数据操作,返回删掉数据的个数
        collection_name：数据库文档名称
        dict_data：删除的数据，格式为字典        
        示例：delete('test_collection',{'a':'b'})
        """
        if collection_name in self.__conn.collection_names():
            collection=self.__conn[collection_name]
            try:
                result=collection.delete_many(dict_data)
                return result.deleted_count
            except:
                traceback.print_exc()
        else:
            print 'collection does not exist'
            return 0
            
    def find(self,collection_name,condition={}):
        """
        向已有的collection查询数据操作，返回查询数据列表
        collection_name：数据库文档名称
        condition：查询条件        
        示例：find('test_collection',{'a':'b'})
        """
        if collection_name in self.__conn.collection_names():        
            collection = self.__conn[collection_name]
            lst=list()
            for item in collection.find(condition):
                lst.append(item)
            if len(lst)==0:
                print 'no records'
            return lst                
        else:
            print 'collection does not exist'
            return 0
            
    def update(self,collection_name,dict_data1,dict_data2):
        """
        向已有的collection修改数据操作，返回修改数据个数
        collection_name：数据库文档名称
        dict_data1：修改前的数据，格式为字典
        dict_data2：修改后的数据，格式为字典
        示例：update('test_collection',{'a':'b'},{'a':'c'})
        """
        if collection_name in self.__conn.collection_names():
            collection = self.__conn[collection_name]
            try:
                result=collection.update_many(dict_data1,{'$set':dict_data2})
                return result.modified_count
            except:
                traceback.print_exc()
        else:
            print 'collection does not exist'
            return 0
            
    def close(self):
        """
        关闭连接
        """
        self.__conn.client.close()   
            

class XML(object):
    """
    XML操作，传人数据格式为XML字符串
    示例 ：
    data = '''
        <person>
            <name>Chuck</name>
            <phone type="intl">
            +1 734 303 4456
            </phone>
            <email hide="yes"/>
        </person>
        '''
    xml = XML(data)
    """
    def __init__(self,data):
        self.root = ET.fromstring(data)
        
    def getcolname(self):
        """
        将外汇行情列名转化为字典，返回值为列表，如{'code':'代码'}
        """
        colname = self.root.find('colname')
        if colname is not None:
            return reduce(lambda x,y:dict(x,**y),[{iner.tag:iner.text} for iner in colname.getiterator()][1:])
    
    def getbank(self):
        """
        将银行名转化为字典，返回值为列表，如{'BOC':'中国银行'}
        """
        bank = self.root.find('bank')
        if bank is not None:
            return reduce(lambda x,y:dict(x,**y),[{iner.tag:iner.text} for iner in bank.getiterator()][1:])
        
        
        
        
        
        