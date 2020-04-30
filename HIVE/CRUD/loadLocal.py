#Loading data from local into hive
import pyhive
import sys
from thrift.Thrift import TType
from pyhive import hive

import pandas as pd

conn = hive.Connection(host='localhost', port=10000, auth='NOSASL', database='default', username='hduser')
cursor = conn.cursor()

#Path of file stored in local
query='''load data local inpath "/home/hduser/user_info.csv" into table userinfo '''
cursor.execute(query)
print("File loaded successfully")