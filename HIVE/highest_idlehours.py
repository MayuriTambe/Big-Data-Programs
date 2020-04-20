import pyhive
import pandas as pd
from pyhive import hive
import sys

#Creating connection to hive server and connecting database
connection = hive.Connection(host='localhost', port=10000, auth='NOSASL', database='users')

#Loading database into hive
userinfo = pd.read_sql('select * from userinfo', connection)

#Converting to datetime format
userinfo['userinfo.idle_time'] = pd.to_datetime(userinfo['userinfo.idle_time'])

#Calaculating total idle hours mean
idle_hours_data = userinfo[userinfo['userinfo.idle_time'] > userinfo['userinfo.idle_time'].mean()]
print(idle_hours_data)

#Users with highest idle hours
highest_idlehours_users=idle_hours_data['userinfo.user_name']
print(highest_idlehours_users)
