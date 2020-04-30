#Updation of data from table
import pyhive
import sys
from thrift.Thrift import TType
from pyhive import hive
import pandas as pd

conn= hive.Connection(host='localhost', port=10000, auth='NOSASL', database='default', username='hduser')
cursor=conn.cursor()

#Update command is not supported on the columns that are bucketed,here we do bucketing on username so cannot apply update query to username column
query='''UPDATE userinfo set start_time="2019-10-24 08:30:02"
where user_name="samadhanmahajan73@gmail.com"

'''
try:
    cursor.execute(query)
    print("Updated Succesfully")
except:
    print("Not succesfully updated")
conn.commit()
