"""ajaxgoogle.py - Simple bindings to the AJAX Google Search API
(Just the JSON-over-HTTP bit of it, nothing to do with AJAX per se)
http://code.google.com/apis/ajaxsearch/documentation/reference.html#_intro_fonje
brendan o'connor - gist.github.com/28405 - anyall.org"""
try:
  import json
except ImportError:
  import simplejson as json

import urllib, urllib2
import os.path

BASE_TEMPLATE = "http://ajax.googleapis.com/ajax/services/search/web?v=1.0"

def refresh_key(key):
    return BASE_TEMPLATE + "&key=" + key
    
def search_url(q, template=None, **kwds):
  if template: url = template
  else: url = TEMPLATE
  url += "&q=" + urllib.quote(q)
  if kwds:
    url += "&" + urllib.urlencode(kwds)
  print url
  return url

def search(q, key=None, **kwds):
  """See options at http://code.google.com/apis/ajaxsearch/documentation/reference.html#_intro_fonje including:
    rsz= large | small  (8 vs 4)
    start= <0-indexed offset>
    hl= <language of searcher>
    lr= <language of results>
    safe= active | moderate | off
  """
  if not key: raise Exception, 'No API Key Provided'
  template = self.refresh_key(key)
  f = urllib2.urlopen(search_url(q, template, **kwds))
  ret = json.load(f)
  if ret['responseStatus']==200 and ret['responseData']:  return ret['responseData']
  raise Exception("Google API error %s: %s\n (%s)" % (ret['responseStatus'],ret['responseDetails'],repr(ret)))


if __name__=='__main__':
  import sys
  from pprint import pprint
  if len(sys.argv) > 1:
    q = " ".join(sys.argv[1:])
  else: q = "query a question to whom in what sense"
  pprint(search(q))