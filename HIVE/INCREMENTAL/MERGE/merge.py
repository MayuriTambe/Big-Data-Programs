import pyhive
import sys
from thrift.Thrift import TType
from pyhive import hive
import pandas as pd

conn= hive.Connection(host='localhost', port=10000, auth='NOSASL', database='default', username='hduser')
cursor=conn.cursor()

'''MERGE statement will first check if row is available in hive table ,it will be updated if it is available 
otherwise new record will be inserted
here two tables are created load_source and load_target,The modified_date column is updated and a new id column is inserted in result'''

query=''' MERGE INTO load_target trg 
    USING load_source src
    ON src.id = trg.id
    WHEN MATCHED THEN
    UPDATE SET modified_date = src.modified_date
    WHEN NOT MATCHED THEN 
    INSERT VALUES (src.id,src.user_name,src.idle_time,src.working_hours,src.start_time,src.end_time,src.modified_date)

'''

cursor.execute(query)
