import pymysql

description = ('name','type_code','display_size','internal_size','precision','scale','null_ok')

def get_template(lens,meta=None):
	template = '\xe2\x94\x83 ' + ' \xe2\x94\x83 '.join(["{}"] * len(lens)) + ' \xe2\x94\x83'
	scales = [""] * len(lens)
	if meta:
		scales = [ str(m[5]) if m[5] else "" for m in meta ]
	wids = [ ("{:"+str(lens[w])+".2f}" if scales[w] else "{:"+str(lens[w])+"}") for w in xrange(len(lens)) ]
	template = template.format(*wids)
	return template

def pretty_table(lens,head,data,row_template,head_template,null=None):
	top = '\xe2\x94\x8f\xe2\x94\x81' + '\xe2\x94\x81\xe2\x94\xb3\xe2\x94\x81'.join([ '\xe2\x94\x81'*w for w in lens ]) + '\xe2\x94\x81\xe2\x94\x93'
	mid = '\xe2\x94\xa3\xe2\x94\x81' + '\xe2\x94\x81\xe2\x95\x8b\xe2\x94\x81'.join([ '\xe2\x94\x81'*w for w in lens ]) + '\xe2\x94\x81\xe2\x94\xab'
	sep = '\xe2\x94\xa0\xe2\x94\x80' + '\xe2\x94\x80\xe2\x95\x82\xe2\x94\x80'.join([ '\xe2\x94\x80'*w for w in lens ]) + '\xe2\x94\x80\xe2\x94\xa8'
	bot = '\xe2\x94\x97\xe2\x94\x81' + '\xe2\x94\x81\xe2\x94\xbb\xe2\x94\x81'.join([ '\xe2\x94\x81'*w for w in lens ]) + '\xe2\x94\x81\xe2\x94\x9b'
	# row_template = unicode(row_template)
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
				row = [ val if val is not None else null for val in row ]
			print row_template.format(*row)
	print bot

def pretty(cursor):
	data = list(cursor)
	if hasattr(cursor,'description'):
		meta = cursor.description[:]
		head = [ col[0] for col in meta ]
	else:
		meta = None
		head = description
	lens = [ len(h) for h in head ]
	width = len(head)
	for row in data:
		for c in xrange(width):
			l = len(str(row[c]))
			if l > lens[c]:
				lens[c] = l
	row_template = get_template(lens,meta)
	head_template = get_template(lens)
	pretty_table(lens,head,[data],row_template,head_template,"")

def lodify(cursor):
	"""convert cursor into a 'lod', or 'list of dicts'
	"""
	data = list(cursor)
	if hasattr(cursor,'description'):
		meta = cursor.description[:]
		head = [ col[0] for col in meta ]
	else:
		meta = None
		head = description
	result = []
	for record in data:
		row = {}
		for c in xrange(len(head)):
			key = head[c]
			val = record[c]
			row[key] = val
		result.append(row)
	return result