import pymysql
from decimal import Decimal
from table import lodify, pretty
from settings import db_params
from sql import stringify
# db = pymysql.connect(**db_params)

def standardize(lod):
	columns = set([])
	for record in lod:
		columns |= set(record.keys())
	columns = list(columns)
	data = []
	for record in lod:
		data.append([ record.get(key) for key in columns])
	return (columns, data)

def get_sql_value(value, url=False):
	if value is None:
		return 'NULL'
	elif type(value) in [int,float,Decimal]:
		return str(value)
	elif type(value) is str:
		return '"'+value+'"'
	elif type(value) is unicode:
		return '"'+value+'"'
	elif type(value) is bool:
		return str(value).upper()
	else:
		return str(value)

def get_sql_row(values):
	values = [ get_sql_value(value) for value in values]
	values = ', '.join(values)
	return "({})".format(values)

def get_where_row(items):
	items = [ "`{}` = {}".format(key,get_sql_value(val)) for key, val in items ]
	where = ' AND '.join(items)
	return where

def add_access(lod):
	for d in lod:
		d['access'] = True
	return lod

def remove_access(lod):
	for d in lod:
		del d['access']
	return lod


class Table(object):
	"""docstring for Table"""
	def __init__(self, db, name):
		self.db = db
		self.name = name
		get_columns = self.db.cursor()
		query = "DESCRIBE {table};".format(table=self.name)
		print query
		get_columns.execute(query)
		get_columns.close()
		# pretty(get_columns)
		self.columns = lodify(get_columns)

	def __exit__(self):
		self.closeall()

	def closeall(self):
		CloseAccess = self.db.cursor()
		query = 'UPDATE {table} SET `access` = FALSE WHERE `access` = TRUE;'
		query = query.format(table=self.name)
		print query
		CloseAccess.execute(query)
		CloseAccess.close()

	def insertlod(self, lod):
		"""Insert a List Of Dicts into table and return a List Of Dicts"""
		cursor = self.InsertLOD(lod)
		result = remove_access(lodify(cursor))
		cursor.close()
		return result

	def InsertLOD(self, lod, keepopen=False):
		lod = add_access(lod)
		cols, data = standardize(lod)
		query = 'INSERT INTO {table} ( `{column_list}` ) VALUES {values};'
		joiner = ', '
		if len(data) > 1:
			query = 'INSERT INTO {table} ( `{column_list}` ) VALUES \n\t{values};'
			joiner = ',\n\t'
		data = [ get_sql_row(datum) for datum in data ]
		data = joiner.join(data)
		column_list = '`, `'.join(cols)
		query = query.format(table=self.name,values=data,column_list=column_list)
		query = query.replace(",)",")")
		
		MainQuery = self.db.cursor()
		print query
		MainQuery.execute(query)
		print '--<',MainQuery.rowcount

		query = 'SELECT * FROM {table} WHERE `access` = TRUE;'
		query = query.format(table=self.name)
		print query
		ResultQuery = self.db.cursor()
		ResultQuery.execute(query)
		print '-->',ResultQuery.rowcount

		if not keepopen:
			self.closeall()

		return ResultQuery

	def select(self, **kwery):
		cursor = self.Select(**kwery)
		result = remove_access(lodify(cursor))
		cursor.close()
		return result

	def Select(self, **kwery):
		MainQuery = self.db.cursor()

		LIMIT = None
		if 'LIMIT' in kwery:
			LIMIT = kwery['LIMIT']
			del kwery['LIMIT']

		query = 'SELECT * FROM {table} WHERE {where}'
		if LIMIT is not None:
			query += ' LIMIT ' + str(LIMIT)
		where = get_where_row(kwery.items())
		query = query.format(table=self.name, where=where)
		query += ';'
		print query
		MainQuery.execute(query)
		print '-->',MainQuery.rowcount
		return MainQuery

	def select_or_insert(self, **data):
		cursor = self.SelectOrInsert(mand=data, opt={})
		result = remove_access(lodify(cursor))[0]
		cursor.close()
		return result

	def SelectOrInsert(self, mand, opt):
		mand['LIMIT'] = 1
		Select = self.Select(**mand)
		if Select.rowcount:
			return Select
		else:
			del mand['LIMIT']
			mand.update(opt)
			return self.InsertLOD([mand])

	def SIM(self, field, values):
		"""Select or Insert Multiple, return cursor"""
		cleanvals = ','.join([ stringify(value) for value in values ])
		values = set(values)
		Already = self.db.cursor()
		query = 'SELECT `{field}` FROM {table} WHERE `{field}` IN ({cleanvals});'
		query = query.format(table=self.name, field=field, cleanvals=cleanvals)
		print query
		Already.execute(query)
		print '-->',Already.rowcount
		new = values - set(Already)
		Already.close()

		self.InsertLOD([ {field:val, 'access':True} for val in new ], keepopen=True)

		query = 'SELECT * FROM {table} WHERE `access` = TRUE;'
		query = query.format(table=self.name)
		print query
		ResultQuery = self.db.cursor()
		ResultQuery.execute(query)
		print '-->',ResultQuery.rowcount

		return ResultQuery

	def sim(self, field, values):
		cursor = self.SIM(field, values)
		result = remove_access(lodify(cursor))
		cursor.close()
		self.closeall()
		return result

	def insert1(self, **data):
		return self.insertlod([data])[0]

	# def consolidate(self, on):
	# 	pass


# Crawl = Table(db, 'Crawl')
# m = Crawl.insert1(cid=212)
# print m

# print Crawl.insertlod([
# 	{
# 		'rootWID': 1,
# 		'nLevels':10,
# 	},
# 	{
# 		'nLevels':11,
# 	},
# ])


class Record(object):
	"""docstring for Record"""
	def __init__(self, table, pkid, data):
		self.table = table
		self.pkid = pkid
		self.data = data
	def populate(self):
		pass

		
# class QuerySet(object):
# 	"""docstring for QuerySet"""
# 	def __init__(self, table, records):
# 		self.table = table
# 		self.records = records
# db.close()