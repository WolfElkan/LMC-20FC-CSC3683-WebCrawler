import pymysql
import datetime

description = ('name','type_code','display_size','internal_size','precision','scale','null_ok')

data_types = {
	   0: 'DECIMAL', 
	   1: 'TINY', 
	   2: 'SHORT', 
	   3: 'LONG', 
	   4: 'FLOAT', 
	   5: 'DOUBLE', 
	   6: 'NULL', 
	   7: 'TIMESTAMP', 
	   8: 'LONGLONG', 
	   9: 'INT24', 
	  10: 'DATE', 
	  11: 'TIME', 
	  12: 'DATETIME', 
	  13: 'YEAR', 
	  14: 'NEWDATE', 
	  15: 'VARCHAR', 
	  16: 'BIT', 
	 128: 'no_change',
	 245: 'JSON', 
	 246: 'NEWDECIMAL', 
	 247: 'INTERVAL', 
	 248: 'SET', 
	 249: 'TINY_BLOB', 
	 250: 'MEDIUM_BLOB', 
	 251: 'LONG_BLOB', 
	 252: 'BLOB', 
	 253: 'VAR_STRING', 
	 254: 'STRING', 
	 255: 'GEOMETRY'
}

def get_head(meta):
	if meta:
		return [ col[0] for col in meta ]
	else:
		return description

def get_template(lens,meta=None):
	template = '\xe2\x94\x83 ' + ' \xe2\x94\x83 '.join(["{}"] * len(lens)) + ' \xe2\x94\x83'
	scales = [""] * len(lens)
	if meta:
		scales = [ str(m[5]) if m[5] else "" for m in meta ]
	wids = [ ("{:"+str(lens[w])+".2f}" if scales[w] else "{:"+str(lens[w])+"}") for w in xrange(len(lens)) ]
	template = template.format(*wids)
	return template

def display(row, c, null, meta=None):
	val = row[c]
	if val is None:
		return null
	type_code = 128
	if meta:
		type_code = meta[c][1]
	if type_code in [12]:
		return str(val)
	return val

def pretty_table(lens,meta,data,row_template,head_template,null=None):
	top = '\xe2\x94\x8f\xe2\x94\x81' + '\xe2\x94\x81\xe2\x94\xb3\xe2\x94\x81'.join([ '\xe2\x94\x81'*w for w in lens ]) + '\xe2\x94\x81\xe2\x94\x93'
	mid = '\xe2\x94\xa3\xe2\x94\x81' + '\xe2\x94\x81\xe2\x95\x8b\xe2\x94\x81'.join([ '\xe2\x94\x81'*w for w in lens ]) + '\xe2\x94\x81\xe2\x94\xab'
	sep = '\xe2\x94\xa0\xe2\x94\x80' + '\xe2\x94\x80\xe2\x95\x82\xe2\x94\x80'.join([ '\xe2\x94\x80'*w for w in lens ]) + '\xe2\x94\x80\xe2\x94\xa8'
	bot = '\xe2\x94\x97\xe2\x94\x81' + '\xe2\x94\x81\xe2\x94\xbb\xe2\x94\x81'.join([ '\xe2\x94\x81'*w for w in lens ]) + '\xe2\x94\x81\xe2\x94\x9b'
	head = get_head(meta)
	print top
	print head_template.format(*head)
	print mid
	g = 0
	for group in data:
		if g:
			print sep
		g += 1
		for row in group:
			if null is not None:
				row = [ display(row,c,null,meta) for c in xrange(len(row)) ]
			print row_template.format(*row)
	print bot

def pretty(cursor):
	data = list(cursor)
	if hasattr(cursor,'description'):
		meta = cursor.description[:]
	else:
		meta = None
	head = get_head(meta)
	lens = [ len(h) for h in head ]
	width = len(head)
	for row in data:
		for c in xrange(width):
			l = len(str(row[c]))
			if l > lens[c]:
				lens[c] = l
	row_template = get_template(lens,meta)
	head_template = get_template(lens)
	pretty_table(lens,meta,[data],row_template,head_template,"")

def lodify(cursor):
	"""convert cursor into a 'lod', or 'list of dicts'
	"""
	data = list(cursor)
	if hasattr(cursor,'description'):
		meta = cursor.description[:]
	else:
		meta = None
	head = get_head(meta)
	result = []
	for record in data:
		row = {}
		for c in xrange(len(head)):
			key = head[c]
			val = record[c]
			row[key] = val
		result.append(row)
	return result