import pyhive
import sys
from thrift.Thrift import TType
from pyhive import hive
import pandas as pd

conn = hive.Connection(host='localhost', port=10000, auth='NOSASL', database='default', username='hduser')
cursor = conn.cursor()

#Creating table in TEXTFILE beacause its not possible to directly load data in ORC formatted table

query = '''create table userinfo_text(user_name STRING,idle_time TIMESTAMP,working_hours TIMESTAMP,start_time
TIMESTAMP,end_time TIMESTAMP)
     clustered by (user_name) into 4 buckets
     stored as TEXTFILE tblproperties('transactional'='true')


'''
cursor.execute(query)