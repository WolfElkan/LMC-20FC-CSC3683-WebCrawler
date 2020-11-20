import urllib2, re, datetime, pymysql
from bs4 import BeautifulSoup
# from sql import insert, insert1, select_or_insert
from sql import clean, stringify
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

def get_soup(url):
	# print url
	try:
		page = urllib2.urlopen(url)
	except Exception as e:
		print '--', e, url
		return False
	else:
		soup = BeautifulSoup(page.read(), 'html.parser')
		return soup

def get_links(soup, url, re=r'^.*$'):
	if not soup:
		return set([])
	return set([ clean(urllib2.urlparse.urljoin(url,a.attrs.get('href'))) for a in soup.findAll('a') ])

porter = PorterStemmer()

def stem(p):
	# print p
	m = re.match(r"^.*?([A-Za-z0-9']+).*$", p)
	# m = re.match(r"^\s*[<(]*(\w*?)[.,?!:)>/]*\s*$", p)
	if m:
		# print m.groups()
		p = m.groups()[0]
	p = porter.stem(p, 0, len(p)-1).upper()
	p = clean(p)
	if '\\' in p or max([ ord(c) for c in p ] + [0]) > 127:
		return ''
	# if '.' in p:
	# 	return stem(re.match(r"^(.*?)\.", p).groups()[0].lower())
	# # 	return stem(re.match(r"^(.*?)\xe2", p).groups()[0].lower())
	# ultraascii = re.match(r"^(.*?)[^[:ascii:]]", p)
	# if ultraascii:
	# 	return stem(ultraascii.groups()[0].lower())
	return p[:25]

def parse_text(text, oid):
	words = [ stem(word) for word in re.split(r'\s+',text) ]
	words = enumerate(words)
	words = filter(lambda word: word, words)
	words = filter(lambda word: word[1] not in default, words)
	words = [ (pos, str(word)) for pos, word in words ]
	words = [ (pos, Stem.select_or_insert(stem=word)['stem']) for pos, word in words ]
	words = [ {
		'stem':word,
		'oid':oid,
		'pos':pos,
	} for pos, word in words ]
	return Word.insertlod(words)

def mine(url, cid, regex=r'^.*$', wid=None, quiet=False):
	soup = get_soup(url)
	if soup:

		if wid is None:
			wid = Webpage.select_or_insert(url=url)['WID']

		# Record the mined data
		oid = Observation.insert1(
			WID=wid, 
			CID=cid, 
			# html=unicode(soup.text), 
			# quiet=True,
		)['OID']
		parse_text(soup.text, oid)

		# Record that this link has been mined
		Update = db.cursor()
		query = 'UPDATE Webpage SET mined=True WHERE wid IN ({});'.format(str(wid))
		if not quiet:
			print query
		Update.execute(query)
		Update.close()

		links = get_links(soup, url)
		links = set(filter(lambda link: re.match(regex, link), links))

		links = [ link.replace('"','%22') for link in links ]

		if links:
			pages = Webpage.sim('url', links)
			Link.insertlod([ {'fromWID':wid,'toWID':row['WID']} for row in pages ])

			# exit()

		# if links:
		# 	SelectWID = db.cursor()
		# 	sqlinks = ','.join([ stringify(link.replace('"','%22')) for link in links ])
		# 	query = 'SELECT WID FROM Webpage WHERE url IN ({});'.format(sqlinks)
		# 	print query
		# 	SelectWID.execute(query)
		# 	print
		# 	already = set(SelectWID)
		# 	# pretty(SelectWID)
		# 	SelectWID.close()

		# 	links - already
		
		# 	exit()
		# 	Link.insertlod([ {'fromWID':wid,'toWID':row} for row in links ])

		# if links:
		# 	SelectURL = db.cursor()
		# 	sqlinks = ','.join([ stringify(link) for link in links ])
		# 	query = 'SELECT url FROM Webpage WHERE url IN ({});'.format(sqlinks)
		# 	if not quiet:
		# 		print query
		# 	SelectURL.execute(query)
		# 	SelectURL.close()

		# 	already = set([ row[0] for row in cursor ])
		# 	links -= already

		# if links:
		# 	sqlinks = ','.join([ stringify(link) for link in links ])
		# 	Webpage.insert([ {'url':str(url),'newCID':cid} for url in links ])

	return soup

def crawl(root,regex=r'^.*$',level=1,quiet=False):

	RootPageID = Webpage.select_or_insert(url=root)['WID']
	CrawlID = Crawl.insert1(rootwid=RootPageID, nLevels=level)['CID']

	soup = mine(root, cid=CrawlID, wid=RootPageID, quiet=quiet, regex=regex)

	discovered = get_links(soup, root)
	discovered = filter(lambda link: re.match(regex, link), discovered)
	discovered_wids = set([])
	for url in discovered:
		curl = clean(url)
		discovered_wids.add(Webpage.select_or_insert(url=str(curl))['WID'])
	discovered_wids = list(discovered_wids)
	discovered_wids.sort()
	discovered_wids = [ str(wid) for wid in discovered_wids ]
	if len(discovered_wids):
		UpdateWebpage = db.cursor()
		query = 'UPDATE Webpage SET newCID={cid} WHERE wid IN ({wids});'.format(cid=CrawlID, wids=','.join(discovered_wids))
		if not quiet:
			print query
		UpdateWebpage.execute(query)
		UpdateWebpage.close()
	else:
		print '-- No links'
	Link.insertlod([ {'fromWID':RootPageID, 'toWID':url} for url in discovered_wids ])

	while level > 0:
		SelectURL = db.cursor()
		query = 'SELECT wid, url FROM Webpage WHERE newCID = {cid} AND mined=False;'.format(cid=CrawlID)
		if not quiet:
			print query
		SelectURL.execute(query)
		for wid, url in SelectURL:
			# print url
			mine(url, cid=CrawlID, wid=wid, quiet=quiet, regex=regex)
			# crawl(url,level-1,CrawlID,quiet=quiet)
		level -= 1
		print '-- Level:', level
		SelectURL.close()
	FinalUpdate = db.cursor()
	query = 'UPDATE Crawl SET endtime="{now}" WHERE cid = {cid};'.format(cid=CrawlID, now=datetime.datetime.now())
	if not quiet:
		print query
	FinalUpdate.execute(query)
	FinalUpdate.close()
	db.close()

# url = 'https://bulbapedia.bulbagarden.net/wiki/Main_Page'
# url = 'https://pokemon.fandom.com/wiki/List_of_Pok%C3%A9mon'
url = 'https://www.landmark.edu/'

def docrawl(url):
	try:
		# crawl(url, quiet=False, level=10, regex=r'^https?://pokemon\.fandom\.com/wiki/[A-Za-z0-9]*')
		crawl(url, level=10, regex=r'^https?://www\.landmark\.edu/*')
		pass
	except Exception as e:
		raise
	finally:
		print 
		print '*'*88
		print
		cursor = db.cursor()
		cursor.execute('SELECT CID FROM Crawl WHERE isnull(endtime) ORDER BY starttime DESC LIMIT 1;')
		CrawlID = cursor.fetchone()
		if CrawlID:
			CrawlID = CrawlID[0]
			query = 'UPDATE Crawl SET endtime="{now}" WHERE cid = {cid};'.format(cid=CrawlID, now=datetime.datetime.now())
			print query
			cursor.execute(query)

		Crawl.__exit__()
		Link.__exit__()
		Observation.__exit__()
		Webpage.__exit__()
		Word.__exit__()
		Stem.__exit__()
		cursor.close()
		db.close()

docrawl(url)

# print max([ ord(c) for c in p ]) > 127
