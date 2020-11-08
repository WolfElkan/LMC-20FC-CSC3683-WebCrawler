import pymysql
from sql import db_params
from table import pretty
db = pymysql.connect(**db_params)
cursor = db.cursor()

query = 'SELECT fromWID, toWID FROM Link'
cursor.execute(query)
pretty(cursor)

db.close()