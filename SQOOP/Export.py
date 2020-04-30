import os
import sys

SqoopExport="sqoop export --connect jdbc:mysql://localhost:3306/sqoopDB --username hduser --password hduser --table userinfo --export-dir /user/hadoop/data/users_info.csv"

os.system(SqoopExport)
