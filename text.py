from utf import escape

def clean(text):
	return "".join(i if ord(i)<128 else escape(ord(i)) for i in text)

def clean_url(text):
	return "".join(i if ord(i)<128 else escape(ord(i),'%').upper() for i in text)

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