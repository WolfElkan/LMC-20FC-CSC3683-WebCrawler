import urllib2, re, datetime, pymysql
from bs4 import BeautifulSoup
# from sql import db_params, insert, insert1, stringify, clean, select_or_insert
import sql
from table import pretty, lodify
from parse_url import parse_url, rebuild
from stem import PorterStemmer
from stopwords import default, cap

db = pymysql.connect(**sql.db_params)
cursor = db.cursor()

print sql.insert1(db, 'Crawl')

db.close()