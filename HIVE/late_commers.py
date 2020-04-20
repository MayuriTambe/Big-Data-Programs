import pyhive
import pandas as pd
from pyhive import hive
import sys

#Creating connection to hive server and connecting database
connection = hive.Connection(host='localhost', port=10000, auth='NOSASL', database='users')

#Loading database into hive
userinfo = pd.read_sql('select * from userinfo', connection)

#Classifying late commers whose start time is more as given condition
late_commers = userinfo[userinfo['userinfo.start_time'] > '2019-10-24 09:30:02']

#Classifying the late commers by their user_name
late_commers_username =late_commers['userinfo.user_name']
print(late_commers_username)