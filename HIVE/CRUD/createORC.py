import pyhive
import sys
from thrift.Thrift import TType
from pyhive import hive
import pandas as pd

conn= hive.Connection(host='localhost', port=10000, auth='NOSASL', database='default', username='hduser')
cursor=conn.cursor()

#Creating table in ORC(Optimized Row Columnar) and performing insertion,deletion and updation on ORC formatted table
'''Hive partition divides table into number of partitions and these partitions can be further subdivided into more manageable parts known as Buckets or Clusters. The Bucketing concept is based on Hash function, which depends on the type of the bucketing column. Records which are bucketed by the same column will always be saved in the same bucket.
Here, clustered by  clause is used to divide the table into buckets.'''


query='''create table userinfo(user_name STRING,idle_time TIMESTAMP,working_hours TIMESTAMP,start_time
TIMESTAMP,end_time TIMESTAMP)
     clustered by (user_name) into 4 buckets
     stored as orc tblproperties('transactional'='true')
'''
cursor.execute(query)
print("Table Created")
conn.close()
