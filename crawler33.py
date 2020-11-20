import urllib2, re, datetime, pymysql
from bs4 import BeautifulSoup
# from sql import insert, insert1, stringify, clean, select_or_insert
from settings import db_params
from table import pretty, lodify
from parse_url import parse_url, rebuild
from stem import PorterStemmer
from stopwords import default, cap
from record import Table

default = cap(default)

db = pymysql.connect(**db_params)

Crawl = Table(db, 'Crawl')
Link = Table(db, 'Link')
Observation = Table(db, 'Observation')
Webpage = Table(db, 'Webpage')
Word = Table(db, 'Word')
Stem = Table(db, 'Stem') 

m = Webpage.SIM('url',['asdf'])
for x in m:
	print x

db.close()