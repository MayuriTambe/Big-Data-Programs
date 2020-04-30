#Loading data from hdfs in hive
import pyhive
import sys
from thrift.Thrift import TType
from pyhive import hive

import pandas as pd

conn = hive.Connection(host='localhost', port=10000, auth='NOSASL', database='default', username='hduser')
cursor = conn.cursor()

#create directory in hdfs and put textfile in directory
query='''load data inpath "/loaddata/file/user_info.csv" into table userinfo '''
cursor.execute(query)
print("File loaded successfully")