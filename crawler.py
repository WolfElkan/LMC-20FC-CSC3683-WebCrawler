import urllib2, re, datetime, pymysql
from bs4 import BeautifulSoup
from sql import db_params, insert, insert1
from table import pretty, lodify
from parse_url import parse_url, rebuild

db = pymysql.connect(**db_params)
cursor = db.cursor()

urls = [
	"https://www.landmark.edu",
	"https://www.landmark.edu/admissions/homeschooled-students",
	"https://colab.research.google.com/notebooks/intro.ipynb?foo=bar&one=i&two=ii&three=iii#scrollTo=GJBs_flRovLc",
	"https://www.dailymail.co.uk/tvshowbiz/article-8317281/Salma-Hayek-makeup-selfie-opening-physical-insecurities.html",
	"https://www.dropbox.com/s/6hnk5a5k6g5o6lh/web-architecture.pptx?dl=0",
]

# insert(db, 'Webpage', [ parse_url(url) for url in urls ])

def get_soup(url):
	page = urllib2.urlopen(url)
	soup = BeautifulSoup(page.read(), 'html.parser')
	return soup

def get_links(soup, url):
	return set([ urllib2.urlparse.urljoin(url,a.attrs.get('href')) for a in soup.findAll('a') ])

def stringify(value):
	if type(value) in [str,unicode]:
		return '"'+value+'"'
	else:
		return str(value)

def select_or_insert(db, table, **kwery):
	if 'quiet' in kwery:
		quiet = kwery['quiet']
		del kwery['quiet']
	else:
		quiet = False
	cursor = db.cursor()
	query = ', '.join([ '`'+kv[0]+'`='+stringify(kv[1]) for kv in kwery.items() ])
	query = 'SELECT * from {} WHERE {} LIMIT 1'.format(table, query)
	if not quiet:
		print query
	cursor.execute(query)
	if cursor.rowcount:
		return cursor.fetchone()[0]
		# return lodify(cursor)
	else:
		return insert(db, table, [kwery], quiet=quiet)


	

def crawl(root,re=r'.*',quiet=False):
	cursor = db.cursor()

	RootPageID = select_or_insert(db, 'Webpage', url=urls[0], quiet=quiet)
	print RootPageID
	CrawlID = insert1(db, 'Crawl', rootwid=RootPageID)
	soup = get_soup(root)
	print CrawlID
	insert1(db, 'Observation', WID=RootPageID, CID=CrawlID, html=str(soup), quiet=True)

	discovered = get_links(soup, root)
	discovered = [ link for link in discovered ]
	discovered_wids = set([])
	for url in discovered:
		discovered_wids.add(select_or_insert(db, 'Webpage', url=str(url)))
	discovered_wids = list(discovered_wids)
	discovered_wids.sort()
	discovered_wids = [ str(wid) for wid in discovered_wids ]
	query = 'UPDATE Webpage SET newCID={cid} WHERE wid IN ({wids});'.format(cid=CrawlID, wids=','.join(discovered_wids))
	if not quiet:
		print query
	cursor.execute(query)
	# discovered_wids
	# insert(db, 'Link', [ {'fromID':RootPageID, 'toID':select_or_insert(db,'Webpage',url=url)} for url in discovered ])



url = urls[0]
# soup = get_soup(url)
# for x in get_links(soup,url):
# 	print x

print crawl(url,quiet=False)

# insert1(db, 'Observation', WID=1, CID=1)

# cursor.execute('SELECT * FROM Webpage')
# pretty(cursor)

# print insert(db, 'Table', [{'fOo':1,'GaR':2},{'fOO':3,'gAr':7}])

# cursor.execute('SELECT * FROM Webpage')
# pretty(cursor)

# page = urllib2.urlopen(url)
# print dir(page)
# print page.info()

# print insert(db, 'Crawl')

# cursor.execute('SELECT wID, protocol, subdomain, domain, tld, ext, fragment FROM Webpage')
# pretty(cursor.description)
# pretty(cursor)
# cursor.execute('SELECT * FROM Crawl')
# pretty(cursor.description)
# pretty(cursor)

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