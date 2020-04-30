import pyhive
import sys
from thrift.Thrift import TType
from pyhive import hive

import pandas as pd
conn= hive.Connection(host='localhost', port=10000, auth='NOSASL', database='default', username='hduser')
cursor=conn.cursor()
query='''INSERT INTO table userinfo select user_name,idle_time,working_hours,start_time,end_time from userinfo_text

'''

cursor.execute(query)