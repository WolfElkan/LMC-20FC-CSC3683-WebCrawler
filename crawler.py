import urllib2, re, datetime, pymysql
from bs4 import BeautifulSoup
from sql import db_params, insert, insert1, stringify, clean
from table import pretty, lodify
from parse_url import parse_url, rebuild
from stem import PorterStemmer
from stopwords import default, cap

default = cap(default)

db = pymysql.connect(**db_params)
cursor = db.cursor()

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

def select_or_insert(db, table, **kwery):
	print
	print
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
		print 'yes'
		return lodify(cursor)
	else:
		print 'no'
		return insert(db, table, [kwery], quiet=quiet)

porter = PorterStemmer()

def stem(p):
	# p = re.match(r"^.*?[A-Za-z0-9']")
	m = re.match(r"^\s*[<(]*(.*?)[.,?!:)>/]*\s*$", p)
	if m:
		p = m.groups()[0]
	p = porter.stem(p, 0, len(p)-1).upper()[:25]
	p = clean(p)
	return p

def parse_text(text, oid):
	words = [ stem(word) for word in re.split(r'\s+',text) ]
	words = filter(lambda word: word, words)
	words = enumerate(words)
	words = filter(lambda word: word[1] not in default, words)
	words = [ str(word) for word in words ]
	words = [ (word[0], select_or_insert(db, 'Stem', stem=word[1])[0]['stem']) for word in words ]
	words = [ {
		'stem':word[1],
		'oid':oid,
		'pos':word[0],
	} for word in words ]
	return insert(db, 'Word', words)

def mine(url, cid, regex=r'^.*$', wid=None, quiet=False):
	soup = get_soup(url)
	if soup:

		if wid is None:
			wid = select_or_insert(db, 'Webpage', url=url)[0]['WID']

		# Record the mined data
		oid = insert1(db, 'Observation', 
			WID=wid, 
			CID=cid, 
			# html=unicode(soup.text), 
			quiet=True,
		)[0]['OID']
		parse_text(soup.text, oid)

		# Record that this link has been mined
		query = 'UPDATE Webpage SET mined=True WHERE wid IN ({});'.format(str(wid))
		if not quiet:
			print query
		cursor.execute(query)

		links = get_links(soup, url)
		links = set(filter(lambda link: re.match(regex, link), links))

		if links:
			sqlinks = ','.join([ stringify(link.replace('"','%22')) for link in links ])
			query = 'SELECT wid FROM Webpage WHERE url IN ({});'.format(sqlinks)
			if not quiet:
				print query
			cursor.execute(query)
		
		# insert(db, 'Link', [ {'fromWID':str(clean(url)),'toWID':row[0]} for row in cursor ], quiet=quiet)
		insert(db, 'Link', [ {'fromWID':wid,'toWID':row[0]} for row in cursor ], quiet=quiet)

		if links:
			sqlinks = ','.join([ stringify(link) for link in links ])
			query = 'SELECT url FROM Webpage WHERE url IN ({});'.format(sqlinks)
			if not quiet:
				print query
			cursor.execute(query)

			already = set([ row[0] for row in cursor ])
			links -= already

		if links:
			sqlinks = ','.join([ stringify(link) for link in links ])
			insert(db, 'Webpage', [ {'url':str(url),'newCID':cid} for url in links ], quiet=quiet)

	return soup

def crawl(root,regex=r'^.*$',level=1,quiet=False):
	cursor = db.cursor()

	RootPageID = select_or_insert(db, 'Webpage', url=root, quiet=quiet)[0]['WID']
	CrawlID = insert1(db, 'Crawl', rootwid=RootPageID, nLevels=level)[0]['CID']

	soup = mine(root, cid=CrawlID, wid=RootPageID, quiet=quiet, regex=regex)

	discovered = get_links(soup, root)
	discovered = filter(lambda link: re.match(regex, link), discovered)
	discovered_wids = set([])
	for url in discovered:
		curl = clean(url)
		discovered_wids.add(select_or_insert(db, 'Webpage', url=str(curl))[0]['WID'])
	discovered_wids = list(discovered_wids)
	discovered_wids.sort()
	discovered_wids = [ str(wid) for wid in discovered_wids ]
	if len(discovered_wids):
		query = 'UPDATE Webpage SET newCID={cid} WHERE wid IN ({wids});'.format(cid=CrawlID, wids=','.join(discovered_wids))
		if not quiet:
			print query
		cursor.execute(query)
	else:
		print '-- No links'
	insert(db, 'Link', [ {'fromWID':RootPageID, 'toWID':url} for url in discovered_wids ])

	while level > 0:
		query = 'SELECT wid, url FROM Webpage WHERE newCID = {cid} AND mined=False;'.format(cid=CrawlID)
		if not quiet:
			print query
		cursor = db.cursor()
		cursor.execute(query)
		for wid, url in cursor:
			# print url
			mine(url, cid=CrawlID, wid=wid, quiet=quiet, regex=regex)
			# crawl(url,level-1,CrawlID,quiet=quiet)
		level -= 1
		print '-- Level:', level
	query = 'UPDATE Crawl SET endtime="{now}" WHERE cid = {cid};'.format(cid=CrawlID, now=datetime.datetime.now())
	if not quiet:
		print query
	cursor.execute(query)
	db.close()

# url = 'https://bulbapedia.bulbagarden.net/wiki/Main_Page'
# url = 'https://pokemon.fandom.com/wiki/List_of_Pok%C3%A9mon'
url = 'https://www.landmark.edu/'

def docrawl(url):
	try:
		# crawl(url, quiet=False, level=10, regex=r'^https?://pokemon\.fandom\.com/wiki/[A-Za-z0-9]*')
		crawl(url, quiet=False, level=10, regex=r'^https?://www\.landmark\.edu/*')
		pass
	except Exception as e:
		cursor = db.cursor()
		cursor.execute('SELECT CID FROM Crawl WHERE isnull(endtime) ORDER BY starttime DESC LIMIT 1;')
		CrawlID = cursor.fetchone()
		if CrawlID:
			CrawlID = CrawlID[0]
			query = 'UPDATE Crawl SET endtime="{now}" WHERE cid = {cid};'.format(cid=CrawlID, now=datetime.datetime.now())
			print query
			cursor.execute(query)
		db.close()
		raise

docrawl(url)


	# print words
	# words = 

	# words = [ {
	# 	'pos':word[0], 
	# 	'stem':word[1][0]['stem'],
	# 	'oid':oid,
	# } for word in words ]

	# insert(db, 'Word', words)

	# print words
	# for word in words:
	# 	print word
	# for pos, word in enumerate(words):
	# 	s = select_or_insert(db, 'Stem', stem=word)
		



# s = "I'll find you"
s = "The deer shall find fraud and more fraud in the house of the watermelons"
# s = "To be or not to be?  That is the question."
# print parse_text(s,0)
# print insert1(db, 'Stem', stem="ROOF")

# print select_or_insert(db, 'Stem', stem='MONTH')

query = 'SELECT stem, CAST(created_at AS DATETIME) AS created_at FROM Stem'
cursor.execute(query)
pretty(cursor)

query = 'SELECT RID, stem, OID, pos, CAST(created_at AS DATETIME) AS created_at FROM Word'
cursor.execute(query)
pretty(cursor)

# print default


# k = 'antidisestablishmentarianism'
# # while k != stem(k):
# k = stem(k)
# k = stem(k)
# print k

# print help(porter.stem)
# print porter.stem('landmark')


# query = 'SELECT fromWID, toWID FROM Link'
# cursor.execute(query)
# pretty(cursor)


db.close()