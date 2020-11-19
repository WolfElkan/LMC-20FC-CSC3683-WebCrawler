class revlist(list):
	"""docstring for revlist"""
	def __init__(self, *args, **kwargs):
		super(revlist, self).__init__(*args, **kwargs)
	def __str__(self):
		self.reverse()
		result = super(revlist, self).__str__()
		self.reverse()
		return result
	def __iter__(self):
		for x in xrange(len(self)-1,-1,-1):
			yield self[x]

def utf(codepoint):
	bl = codepoint.bit_length()
	if bl < 8:
		byte_count = 1
		return [codepoint]
	elif bl < 12:
		byte_count = 2
	else:
		byte_count = (bl + 3) / 5
	result = []
	for x in xrange(byte_count):
		result = [codepoint % 2**6 + 2**7] + result
		codepoint >>= 6
	result[0] += 128 - 2 ** (8 - byte_count)
	return result

def bits(val):
	bites = revlist()
	while val:
		bites.append(val % 2 ** 8)
		val >>= 8
	return bites

def bitstr(val):
	return '0b' + ' '.join([ str(bin(byte))[2:] for byte in bits(val)])

def escape(codepoint, ec='\\x'):
	return ''.join([ ec+hex(byte)[2:].zfill(2) for byte in utf(codepoint) ])