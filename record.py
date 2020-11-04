class Table(object):
	"""docstring for Table"""
	def __init__(self, db, name):
		super(Table, self).__init__()
		self.db = db
		self.name = name
		

class Record(object):
	"""docstring for Record"""
	def __init__(self, table, pkid, data):
		super(Record, self).__init__()
		self.table = table
		self.pkid = pkid
		self.data = data
	def populate(self):
		pass
		