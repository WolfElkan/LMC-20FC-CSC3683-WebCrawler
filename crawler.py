import urllib, re, datetime, pymysql
from bs4 import BeautifulSoup
from sql import db_params, insert
from table import pretty, lodify
from parse_url import parse_url, rebuild

db = pymysql.connect(**db_params)
cursor = db.cursor()

urls = [
	"https://www.landmark.edu/",
	"https://www.landmark.edu/admissions/homeschooled-students",
	"https://colab.research.google.com/notebooks/intro.ipynb?foo=bar&one=i&two=ii&three=iii#scrollTo=GJBs_flRovLc",
	"https://www.dailymail.co.uk/tvshowbiz/article-8317281/Salma-Hayek-makeup-selfie-opening-physical-insecurities.html",
	"https://www.dropbox.com/s/6hnk5a5k6g5o6lh/web-architecture.pptx?dl=0",
]

# insert(db, 'Webpage', [ parse_url(url) for url in urls ])

def get_urls(html):
	pass

def crawl(root,re=r'.*'):
	insert(db, 'Crawl')

# print insert(db, 'Crawl')

cursor.execute('SELECT wID, protocol, subdomain, domain, tld, ext, fragment FROM Webpage')
pretty(cursor.description)
pretty(cursor)
# cursor.execute('SELECT * FROM Webpage')
# pretty(cursor.description)
# pretty(cursor)
cursor.execute('SELECT * FROM Crawl')
pretty(cursor.description)
pretty(cursor)

# nums = []
# index = {}

# C = pymysql.constants.FIELD_TYPE
# for n in dir(C):
# 	if n[0] != '_':
# 		datatype = str(n)
# 		code = C.__getattribute__(n)
# 		# print code, datatype
# 		nums.append(code)
# 		index[code] = datatype

# nums.sort()

# for x in nums:
# 	print x, index[x]

# print index

db.close()