# -*- coding: utf-8 -*-
import pymysql

# 1.链接数据库
db = pymysql.connect(
      host='localhost',
      port=3306,
      user='sell_data',
      passwd='123456',
      db='sell_data',
      charset='utf8')
# 建立链接游标
cursor = db.cursor()
print ('>> 已连接数据表，处理中...')

# 2.添加数据库表头（创建的字段，不要使用空格）
sql = '''CREATE TABLE IF NOT EXISTS kai_test (
        ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        date_time CHAR(30),
        settlement_id CHAR(30),
        type CHAR(30),
        order_id CHAR(50),
        sku CHAR(30),
        quantity CHAR(10),
        product_sales CHAR(20),
        shipping_credits CHAR(20),
        gift_wrap_credits CHAR(20)
        )'''
cursor.execute(sql)

# 3.提交并关闭链接
cursor.close()
db.close()
print ('>> Done.')


# -*- coding: utf-8 -*-
import pymysql,time
import glob,os
import pandas as pd

# 1.准备，指定目录
time_start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) # 记录当前时间
print('>> 当前时间：',time_start)
print('>> 开始处理……')
filelocation = r"C:/Users/Administrator/Desktop/input/"

# 2.链接数据库
print('>> 连接MySQL...')
db = pymysql.connect(
      host='localhost',
      port=3306,
      user='sell_data',
      passwd='123456',
      db='sell_data',
      charset='utf8')
# 建立链接游标
cursor = db.cursor()
print ('>> 已连接数据表。')

# 3.查看本地新文件名
filenames=[]
os.chdir(filelocation) #指定目录
for i in glob.glob("*.csv"): # 获取指定目标下所有的CSV文件名
    filenames.append(i[:-4]) # 文件名不包含“.csv”
count = len(filenames)
print('>> 本地文件：',count,'个') # 如下是以“Num.**”为序号打印出每个文件名
for i in range(0,count): # 把0-9的数字用0补齐2位，也可以用zfill函数或者format格式化实现
    if i<9:
        ii = i+1
        ij = '0'+str(ii)
    else:
        ij = i+1
    print(' - Num.', end='')
    print(ij, filenames[i])

# 4.把新文件的数据提交mysql
print('>> 读取中...')
# MySQL语句
insert_sql = 'insert into kai_test (date_time,settlement_id,type,order_id,sku,quantity,product_sales,shipping_credits,gift_wrap_credits) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
# 开始逐个文件处理
for file_name in filenames:
    print(" + 正在处理：", file_name,'（第',filenames.index(file_name)+1,'个）')
    time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  # 记录处理每个文件的时间
    print(' - 当前时间：', time_now)
    data_csv = pd.read_csv(open(filelocation + file_name+'.csv')) # 使用Pandas读取数据文件
    # print(data_csv.head(3)) # 查看前3条数据
    # print(data_csv.info()) # 查看数据表信息
    # print(len(data_csv.index)) # 查看数据量
    # print(data_csv.loc[2].values) # 查看指定某一行的数据
    ii = 0 # 用于统计每个文件的数据量
    for i in range(0,data_csv.shape[0]): # 逐行读取
        row = data_csv.loc[i].values # 获取第i行数据
        # print(i,'>>:',data_csv.loc[i].values) # 打印第i行数据
        cursor.execute(insert_sql, (str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]), str(row[5]), str(row[6]), str(row[7]), str(row[8])))
        ii = i + 1
    print(' - 提交数量：',ii,'条')

# 5.结束
db.commit() # 提交记录
db.close() # 关闭db
cursor.close() # 关闭游标
time_finish = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) # 记录当前时间
print('>> 当前时间：',time_finish)
print('\n',end='')
print('>> Done.') #完毕