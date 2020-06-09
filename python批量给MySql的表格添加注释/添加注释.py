# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine
import pymysql
import warnings
warnings.filterwarnings("ignore")

engine = create_engine('mysql+pymysql://root:2221@192.168.0.230:3306/edw')
 

# 制作表格： ‘表名’：对应字段的注释
dict_1 = {
        't0':['全网下单转化率', '全网商品数', '全网搜索人气', '全网搜索热度', 
                               '全网点击率', '关键词', '周期', '商城点击占比', '带来的浏览量', 
                               '带来的访客数', '引导下单买家数', '引导下单转换率', '引导入店人均浏览量', 
                               '引导到店铺内的浏览量', '引导支付商品数', '引导支付金额', '曝光量', 
                               '直通车平均点击单价', '终端', '跳失率', 'start_name', 'end_time', 'gmt_creat'],
    
                               
                't2':['周期', '客单价', '成功退款金额', '支付件数', '支付转化率', '支付金额',
                               '淘宝客佣金', '直通车消耗', '老买家支付金额', '访客数', '钻石展位消耗', 
                               'start_name', 'end_time', 'gmt_creat'],
         }

# 对键值进行遍历，也就是表名
for k in dict_1.keys():
#    print(k)
    sql_2 = """SELECT   concat('alter table ',  table_schema, '.', table_name, ' modify column ', column_name, ' ', column_type, ' ',     
                    if(is_nullable = 'YES', ' ', 'not null '),     
                    if(column_default IS NULL, '',     
                    if( data_type IN ('char', 'varchar')     
                    OR     
                    data_type IN ('date', 'datetime', 'timestamp') AND column_default != 'CURRENT_TIMESTAMP',     
                    concat(' default ''', column_default,''''),     
                    concat(' default ', column_default)    
                    )    
                    ),     
                    if(extra is null or extra='','',concat(' ',extra)),  
                    ' comment ''', column_comment, ''';') sql_t    
                    FROM information_schema.columns    
                    WHERE table_schema = 'edw'    
                    AND table_name = '%s';"""%k


    # 构造sql语句
    df2 = pd.read_sql_query(sql_2, engine)

    sql_list = []
    # .iterrows()对DF返回的值中有序列号
    for index,row in df2.iterrows():
        # 将单引号中的值进行替换，按照字典的<键值+索引>的方式进行替换
        sql_s = list(row.str.replace("''","'%s'"%dict_1[k][index]))
#        print(sql_s)
        # 将构造好的sql语句加载到sql的列表中
        sql_list.extend(sql_s)
        
    for s in sql_list:
        # 进行循环的执行
        engine.execute(s)

print('finished')