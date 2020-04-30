import pyhive
import sys
from thrift.Thrift import TType
from pyhive import hive
import pandas as pd

conn= hive.Connection(host='localhost', port=10000, auth='NOSASL', database='default', username='hduser')
cursor=conn.cursor()
query='''DELETE FROM userinfo where user_name="narsimharaj.kasu01@gmail.com"
'''
cursor.execute(query)
print("")