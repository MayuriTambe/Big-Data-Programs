import pyhive
import sys
from thrift.Thrift import TType
from pyhive import hive
import pandas as pd

conn= hive.Connection(host='localhost', port=10000, auth='NOSASL', database='default', username='hduser')
cursor=conn.cursor()

'''Basically, for combining specific fields from two tables by using values common to each one we use Hive JOIN clause. In other words, to combine records from 
two or more tables in the database we use JOIN clause,we create view beacause
whenever we want this join table then we have run his loading view'''


query='''create view loading as
select t1.* from
(select * from load) t1
join
(select id,max(modified_date) max_modify from
(select * from load) t2
group by id) s
ON t1.id = s.id AND t1.modified_date = s.max_modify
'''

cursor.execute(query)
