import re
url_regex = r'(?P<url>(?P<protocol>\w+):/{1,3}((?P<subdomain>[0-9a-zA-Z_.]{,16})\.|)(?P<domain>\w{3,})\.(?P<tld>xn--\w+|[a-z]{2}\.[a-z]{2}|[a-z]+)(/(?P<pathway>[^?#.]+(\.(?P<ext>\w+))?))?(\?(?P<query>[^#]+))?(#(?P<fragment>\w+))?)'

def parse_url(url):
	match = re.match(url_regex,url)
	if match:
		group = match.groupdict()
	else:
		return None
	if group['pathway']:
		group['pathlist'] = group['pathway'].split('/')
		group['page'] = group['pathlist'][-1]
	if group['query']:
		group['qparams'] = dict([ arg.split('=') for arg in group['query'].split('&') ])
	return group

def desanitize(s):
	s = re.compile(r'[-+_]').sub(' ',s)
	s = re.compile(r'%([0-9A-F]{2})').sub(lambda match: chr(int(match.groups()[0],16)),s)
	return s

def rebuild(parsed):
	if not parsed:
		return None
	parsed['pathway'] = parsed['pathway'] if parsed['pathway'] else ""
	parsed['query'] = "?"+parsed['query'] if parsed['query'] else ""
	parsed['ext'] = "."+parsed['ext'] if parsed['ext'] else ""
	parsed['fragment'] = "#"+parsed['fragment'] if parsed['fragment'] else ""
	return "{protocol}://{subdomain}.{domain}.{tld}/{pathway}{ext}{fragment}".format(**parsed)