import pyhive
import sys
from thrift.Thrift import TType
from pyhive import hive

import pandas as pd
conn= hive.Connection(host='localhost', port=10000, auth='NOSASL', database='default', username='hduser')
cursor=conn.cursor()

query='''CREATE EXTERNAL TABLE load(id STRING,user_name STRING,idle_time STRING,working_hours STRING,start_time
STRING,end_time STRING,modified_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/loading/incremental'
tblproperties("skip.header.line.count"="1")
'''

cursor.execute(query)
