import urllib2, re, datetime, pymysql
from bs4 import BeautifulSoup
from sql import db_params, insert, insert1
from table import pretty, lodify
from parse_url import parse_url, rebuild

db = pymysql.connect(**db_params)
cursor = db.cursor()

def get_soup(url):
	# print url
	try:
		page = urllib2.urlopen(url)
	except Exception as e:
		return False
	else:
		soup = BeautifulSoup(page.read(), 'html.parser')
		return soup


def get_links(soup, url):
	if not soup:
		return set([])
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
	query = 'SELECT * from {} WHERE {} LIMIT 1;'.format(table, query)
	if not quiet:
		print query
	cursor.execute(query)
	if cursor.rowcount:
		return cursor.fetchone()[0]
		# return lodify(cursor)
	else:
		return insert(db, table, [kwery], quiet=quiet)


def mine(url, cid, quiet=False):
	soup = get_soup(url)
	if soup:
		wid = select_or_insert(db, 'Webpage', url=url)
		oid = insert1(db, 'Observation', WID=wid, CID=cid, html=str(soup), quiet=True)
		query = 'UPDATE Webpage SET mined=True WHERE wid={};'.format(str(wid))
		if not quiet:
			print query
		cursor.execute(query)
	return soup

def crawl(root,regex=r'^.*$',level=4,CrawlID=None,quiet=False):
	cursor = db.cursor()

	RootPageID = select_or_insert(db, 'Webpage', url=root, quiet=quiet)
	
	if CrawlID is None:
		CrawlID = insert1(db, 'Crawl', rootwid=RootPageID)

	soup = mine(root, CrawlID, quiet=quiet)

	discovered = get_links(soup, root)
	discovered = filter(lambda link: re.match(regex, link), discovered)
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
	insert(db, 'Link', [ {'fromID':RootPageID, 'toID':url} for url in discovered_wids ])

	while level > 0:
		query = 'SELECT wid, url FROM Webpage WHERE newCID = {cid} AND mined=False;'.format(cid=CrawlID)
		if not quiet:
			print query
		cursor.execute(query)
		for wid, url in cursor:
			print 'level:', level
			# print url
			mine(url, CrawlID, quiet)
			# crawl(url,level-1,CrawlID,quiet=quiet)
		level -= 1
	query = 'UPDATE Crawl SET endtime="{now}" WHERE cid = {cid};'.format(cid=CrawlID, now=datetime.datetime.now())
	if not quiet:
		print query
	cursor.execute(query)
	db.close()

url = 'https://www.landmark.edu/'

try:
	crawl(url,quiet=False, level=10, regex=r'^https?://www\.landmark\.edu.*')
	pass
except Exception as e:
	print datetime.datetime.now()
	db.close()
	raise