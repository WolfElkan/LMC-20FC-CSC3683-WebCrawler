import sys
from crawler32 import docrawl

docrawl(sys.argv[1])

# docrawl('https://www.landmark.edu/',r'^https?://www\.landmark\.edu/*')
# docrawl('https://pokemon.fandom.com/f',r'^https?://pokemon\.fandom\.com/*')
# docrawl('https://www.reddit.com/')
# docrawl('https://www.keene.edu/',r'^https?://www\.keene\.edu/*'))
# docrawl('https://www.moody.edu/',r'^https?://www\.moody\.edu/*')
# docrawl('https://www.harvard.edu/',r'^https?://www\.harvard\.edu/*')
# docrawl('https://www.yale.edu/',r'^https?://www\.yale\.edu/*')
# docrawl('https://www.berkeley.edu/',r'^https?://www\.berkeley\.edu/*')