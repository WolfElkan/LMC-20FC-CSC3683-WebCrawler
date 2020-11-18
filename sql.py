import pymysql
from pw import password
from table import lodify
from utf import escape

db_params = {
	'host':"127.0.0.1",
	'port':3306,
	'user':"root",
	'password':password,
	'database':"Crawler",
	'autocommit':True,
}

tables = {}

db = pymysql.connect(**db_params)
ShowTables = db.cursor()
ShowTables.execute('SHOW TABLES')

def get_columns(table):
	return set(tables[table])


# print dir(cursor)
# print cursor

for table in ShowTables:
	# print table
	DescribeTable = db.cursor()
	DescribeTable.execute('DESCRIBE '+table[0])
	table_columns = [x[0].lower() for x in DescribeTable]
	tables[table[0]] = table_columns

# print tables

def clean(text):
	# text = bytes(text)
	# print text
	return "".join(i if ord(i)<128 else escape(ord(i),'%').upper() for i in text)
	if type(text) is unicode:
		text = str(text)
	# return text

def stringify(value):
	if type(value) is str:
		return '"'+value+'"'
	elif type(value) is unicode:
		return str('"'+clean(value)+'"')
	else:
		return str(value)

def stringify_if_unicode(value):
	if type(value) is unicode:
		return stringify(value)

def insert1(db, table, **data):
	quiet = False
	if 'quiet' in data:
		quiet = data['quiet']
		del data['quiet']
	# print data
	return insert(db, table, [data], quiet=quiet)


# `data` formatted as a lod
def insert(db, table, data=None, quiet=False):
	# Make `data` keys lowercase
	data = [ dict([ (k.lower(), stringify_if_unicode(row[k])) for k in row ]) for row in data ]
	# FastFail
	if data is None or not any([ any(record.values()) for record in data ]):
		query = "INSERT INTO {} VALUES ();".format(table)
	else:
		# FastFail
		if len(data) == 0:
			return None
		data_columns = set([])
		for row in data:
			if row:
				for key in row.keys():
					data_columns.add(key.lower())
		# print 'data', data
		# print 'data columns', data_columns
		# print 'table', table
		table_columns = get_columns(table)
		# print 'table columns', table_columns
		columns = data_columns & table_columns
		columns = list(columns)
		# print 'columns', columns
		values = []
		for row in data:
			valrow = []
			allblank = True
			for key in columns:
				# print row
				if row and key.lower() in row and row[key.lower()] is not None:
					valrow.append(row[key])
					allblank = False
				else:
					valrow.append('NULL')
			if allblank:
				values.append('-- Empty Record')
			else:
				values.append(str(tuple(valrow)))
		query = 'INSERT INTO {} ( `{}` ) VALUES {};'
		joiner = ', '
		if len(data) > 1:
			query = 'INSERT INTO {} ( `{}` ) VALUES \n\t{};'
			joiner = ',\n\t'
		values = joiner.join(values)
		query = query.format(table,'`, `'.join(columns),values)
		query = query.replace("'NULL'","NULL")
		query = query.replace(",)",")")
	MainQuery = db.cursor()
	LastIn = db.cursor()
	lastin = 'SELECT * FROM {} ORDER BY created_at DESC LIMIT {};'.format(table, len(data))
	if not quiet:
		print query, '-- query'
		print lastin, '-- lastin'
	MainQuery.execute(query)
	LastIn.execute(lastin)
	return lodify(LastIn)
	# else:


db.close()

# print insert(cursor,table='Webpage',data=m)