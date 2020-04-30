import os
import sys

SqoopImport="sqoop import --connect jdbc:mysql://localhost:3306/sqoopDB --username hduser --password hduser --table userinfo --m 1 --target-dir /user/hadoop/data/importing"

os.system(SqoopImport)
