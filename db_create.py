'''
db_create
Author: Tristan Miller
This is an executable script that will generate a database from target file.
Expect it to take about 10 minutes and take about 200 MB space.

Example bash script:
python db_create.py "planning-department-records-2018/PPTS_Records_data.csv" "2018Q4.db"
'''

import database_creator
import sys

database_creator.create(sys.argv[1],sys.argv[2])