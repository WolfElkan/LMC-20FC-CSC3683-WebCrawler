import pymysql
from pw import password

db_params = {
	'host':"127.0.0.1",
	'port':3306,
	'user':"root",
	'password':password,
	'database':"Crawler",
	'autocommit':True,
}

def insert(db, table, data=None, columns=set([]),quiet=False):
	if data is None:
		query = "INSERT INTO {} VALUES ();".format(table)
	else:
		if len(data) == 0:
			return 0
		cursor = db.cursor()
		if not columns:
			for row in data:
				if row:
					for key in row.keys():
						columns.add(key)
		cursor.execute('DESCRIBE '+table)
		columns &= set([x[0] for x in cursor])
		columns = list(columns)
		values = []
		for row in data:
			valrow = []
			allblank = True
			for key in columns:
				if row and key in row and row[key] is not None:
					valrow.append(row[key])
					allblank = False
				else:
					valrow.append('NULL')
			if allblank:
				values.append('-- Empty Record')
			else:
				values.append(str(tuple(valrow)))
		query = 'INSERT INTO {} (`{}`) VALUES {};'
		joiner = ', '
		if len(data) > 1:
			query = 'INSERT INTO {} (`{}`) VALUES \n\t{};'
			joiner = ',\n\t'
		values = joiner.join(values)
		query = query.format(table,'`, `'.join(columns),values)
		query = query.replace("'NULL'","NULL")
		query = query.replace(",)",")")
	if not quiet:
		print query
	cursor = db.cursor()
	return cursor.execute(query)


m = [
	{'url':1,'bar':2},
	{'url':3,'jud':8}
]

# print insert(cursor,table='Webpage',data=m)