import pymysql

description = ('name','type_code','display_size','internal_size','precision','scale','null_ok')

def pretty_table(lens,head,data,null=None):
	top = '\xe2\x94\x8f\xe2\x94\x81' + '\xe2\x94\x81\xe2\x94\xb3\xe2\x94\x81'.join([ '\xe2\x94\x81'*w for w in lens ]) + '\xe2\x94\x81\xe2\x94\x93'
	mid = '\xe2\x94\xa3\xe2\x94\x81' + '\xe2\x94\x81\xe2\x95\x8b\xe2\x94\x81'.join([ '\xe2\x94\x81'*w for w in lens ]) + '\xe2\x94\x81\xe2\x94\xab'
	sep = '\xe2\x94\xa0\xe2\x94\x80' + '\xe2\x94\x80\xe2\x95\x82\xe2\x94\x80'.join([ '\xe2\x94\x80'*w for w in lens ]) + '\xe2\x94\x80\xe2\x94\xa8'
	bot = '\xe2\x94\x97\xe2\x94\x81' + '\xe2\x94\x81\xe2\x94\xbb\xe2\x94\x81'.join([ '\xe2\x94\x81'*w for w in lens ]) + '\xe2\x94\x81\xe2\x94\x9b'
	row_template = '\xe2\x94\x83 ' + ' \xe2\x94\x83 '.join(["{}"] * len(head)) + ' \xe2\x94\x83'
	wids = [ "{:"+str(w)+"}" for w in lens ]
	row_template = row_template.format(*wids)
	print top
	print row_template.format(*head)
	print mid
	g = 0
	for group in data:
		if g:
			print sep
		g += 1
		for row in group:
			if null is not None:
				row = [ val if val is not None else null for val in row ]
			print row_template.format(*row)
	print bot

def pretty(cursor):
	data = list(cursor)
	if hasattr(cursor,'description'):
		head = [ col[0] for col in cursor.description ]
	else:
		head = description
	lens = [ len(h) for h in head ]
	width = len(head)
	for row in data:
		for c in xrange(width):
			l = len(str(row[c]))
			if l > lens[c]:
				lens[c] = l
	pretty_table(lens,head,[data],"")

def describe(cursor):
	pretty_table([4, 9, 12, 13, 9, 5, 7],description,[cursor.description])