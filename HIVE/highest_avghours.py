import pyhive
import pandas as pd
from pyhive import hive
import sys

#Creating connection to hive server and connecting database
connection = hive.Connection(host='localhost', port=10000, auth='NOSASL', database='users')

#Loading database into hive
userinfo = pd.read_sql('select * from userinfo', connection)

#Converting to datetime format
userinfo['userinfo.working_hours'] = pd.to_datetime(userinfo['userinfo.working_hours'])

#Calaculating total working hours mean
avg_hours = userinfo[userinfo['userinfo.working_hours'] > userinfo['userinfo.working_hours'].mean()]
print(avg_hours)

#Users with highest avghours hours
highest_avghours_users=avg_hours['userinfo.user_name']
print(highest_avghours_users)
