### 原理：利用sql的concat来组合表格的基本信息

![image-20200609114108589](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20200609114108589.png)

先利用sql进行表格的信息查询来制作执行语句（网上轮子）：

```sql
SELECT     
concat(    
    'alter table ',     
    table_schema, '.', table_name,     
    ' modify column ', column_name, ' ', column_type, ' ',     
    if(is_nullable = 'YES', ' ', 'not null '),     
    if(column_default IS NULL, '',     
        if(    
            data_type IN ('char', 'varchar')     
            OR     
            data_type IN ('date', 'datetime', 'timestamp') AND column_default != 'CURRENT_TIMESTAMP',     
            concat(' default ''', column_default,''''),     
            concat(' default ', column_default)    
        )    
    ),     
    if(extra is null or extra='','',concat(' ',extra)),  
    ' comment ''', column_comment, ''';'    
) '组合语句'    
FROM information_schema.columns    
WHERE table_schema = 'test_1' -- 库名    
    AND table_name = 'h_test' -- 表名
```

这里就查询并且构造了‘test_1’库的‘h_test’表格的信息，查询出来如下：

![image-20200609114447711](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20200609114447711.png)

- 可以看到，已经成功构造了alter执行语句，但没有注释的部分是个单引号，我们接下来就是对单引号的语句进行赋值便可以批量执行了

### 利用Python进行批量的操作

如果库中存在多张表格的注释需要添加，这个时候利用Python进行循环，批量添加：(这里使用sqlalchemy模块来控制数据库)

1、构造库表字段的注释的对照字典格式>>> '表名'：['字段对应的注释']

```python
dict_1 = {
        't0':['全网下单转化率', '全网商品数', '全网搜索人气', '全网搜索热度', 
               '全网点击率', '关键词', '周期', '商城点击占比', '带来的浏览量', 
               '带来的访客数', '引导下单买家数', '引导下单转换率', '引导入店人均浏览量', 
              '引导到店铺内的浏览量', '引导支付商品数', '引导支付金额', '曝光量', '直通车平均点击单价', 
              '终端', '跳失率', 'start_name', 'end_time', 'gmt_creat'],
    ...
    '表名'：['字段对应的注释'],
}
```

2、构造sql语句

```python
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

#    print(sql_2)
    # 构造sql语句
    df2 = pd.read_sql_query(sql_2, engine)
#    print(df2)

    sql_list = []
    for index,row in df2.iterrows():
        # 将单引号中的值进行替换，按照字典的键值+索引的方式进行替换
        sql_s = list(row.str.replace("''","'%s'"%dict_1[k][index]))
#        print(sql_s)
        # 将构造好的sql语句加载到sql的列表中
        sql_list.extend(sql_s)
```

3、将重新构造的sql重新进行执行就可以了（如下图：注释补充完整）

![image-20200609115839668](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20200609115839668.png)

完整代码大家可以点击链接查看：