#!/usr/bin/env python
# encoding: utf-8
"""
utils.py

Created by Syd on 2009-08-22.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""

import sys
import os
import re
from hashlib import md5
import chardet
import feedparser
import random
import time
import traceback
from datetime import datetime
import threading
import Queue
from urlparse import urlsplit, urljoin, urlparse, urlunparse
from eventlet.green.urllib import unquote, quote
from eventlet.api import with_timeout
from eventlet.db_pool import ConnectTimeout
from HTMLParser import HTMLParseError
from BeautifulSoup import BeautifulSoup  # For processing HTML
#from SuperMario.Storage import LightCloud

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders

import urllib
import httplib2
from base64 import b64encode

stderr = sys.stderr

ALT_CODECS = {
  'euc': 'euc-jp',
  'x-euc-jp': 'euc-jp',
  'x-sjis': 'ms932',
  'x-sjis-jp': 'ms932',
  'shift-jis': 'ms932',
  'shift_jis': 'ms932',
  'sjis': 'ms932',
  'gb2312': 'gb18030',
  'gb2312-80': 'gb18030',
  'gb-2312': 'gb18030',
  'gb_2312': 'gb18030',
  'gb_2312-80': 'gb18030',
}

try: 
    from cStringIO import StringIO 
except ImportError: 
    from StringIO import StringIO

def Traceback():
    try:
        s = StringIO() 
        traceback.print_exc(file=s) 
        return s.getvalue()
    except:
        return ''

class URL:
    
    #@staticmethod
    #def is_duplicate(url, lightcloud):
    #    return with_timeout(1, lightcloud.get, LightCloud.crawled_url_key(url), timeout_value=None)
    
    #@staticmethod
    #def been_inserted(url, lightcloud):
    #    return with_timeout(1, lightcloud.get, LightCloud.item_key(url), timeout_value=None)
    
    #@staticmethod
    #def has_rss(url, lightcloud):
    #    return lightcloud.get(LightCloud.crawled_rss_key(url))
        
    @staticmethod
    def norm(urltuple, slashend=True):
        _collapse = re.compile('([^/]+/\.\./?|/\./|//|/\.$|/\.\.$)')
        _server_authority = re.compile('^(?:([^\@]+)\@)?([^\:]+)(?:\:(.+))?$')
        _default_port = {'http': '80', 'https': '443', 'gopher': '70', 'news': '119', 'snews': '563', 'nntp': '119', 'snntp': '563', 'ftp': '21', 'telnet': '23', 'prospero': '191',}
        _relative_schemes = ['http', 'https', 'news', 'snews', 'nntp', 'snntp', 'ftp', 'file', '' ]
        _server_authority_schemes = ['http', 'https', 'news', 'snews', 'ftp',]
        (scheme, authority, path, parameters, query, fragment) = urltuple
        scheme = scheme.lower()
        if authority:
            userinfo, host, port = _server_authority.match(authority).groups()
            if host[-1] == '.':
                host = host[:-1]
            authority = host.lower()
            if userinfo:
                authority = "%s@%s" % (userinfo, authority)
            if port and port != _default_port.get(scheme, None):
                authority = "%s:%s" % (authority, port)
        if scheme in _relative_schemes:
            last_path = path
            while 1:
                path = _collapse.sub('/', path, 1)
                if last_path == path:
                    break
                last_path = path
            path = URL.charconvert(path)
            parameters = URL.charconvert(parameters)
            query = URL.charconvert(query)
            #return (scheme, authority, path, parameters, query, fragment)
            if slashend:
                if not path and not authority.endswith('/'):
                    authority = '%s/'%authority
                if path and not parameters and not query and not path.endswith('/') and not len(path.split('.')) > 1:
                    path = '%s/'%path
            return (scheme, authority, path, parameters, query, '')
    
    @staticmethod
    def charconvert(path):
        if not path: return path
        if isinstance(path, unicode): path = path.encode('utf-8')
        #unquote_path = unquote(path)
        #if path!=unquote_path:
        #    charset = chardet.detect(unquote_path)
        #else:
        charset = chardet.detect(path)
        if charset and charset['encoding']: 
            charset['encoding'] = charset['encoding'].lower()
            if charset['encoding'] in ALT_CODECS: charset['encoding'] = ALT_CODECS[charset['encoding']]
            path = path.decode(charset['encoding'])
        return path

    @staticmethod
    def normalize(url, slashend=True):
        try:
            url = urlunparse(URL.norm(urlparse(url), slashend))
            if isinstance(url, unicode): return url.encode('utf-8')
            return url
        except:
            raise Exception
    
    @staticmethod
    def quote(url):
        (scheme, authority, path, parameters, query, fragment) = urlparse(url)
        query = '&'.join(['='.join([ x for x in q.split('=')]) for q in query.split('&')])
        fragment = quote(fragment)
        return urlunparse((scheme, authority, path, parameters, query, fragment))
        
    @staticmethod
    def baseurl(url):
        (proto, hostport, _x, _y, _z) = urlsplit(url)
        return URL.normalize('%s://%s'%(proto, hostport))
        
    @staticmethod
    def link_title(data, url):
        def bcheck(a, key):
            try:
                return a[key]
            except KeyError:
                return ''
            else:
                return ''
        try:
            soup = BeautifulSoup(data, fromEncoding='utf-8')
            return ((URL.normalize(urljoin(url, a['href'])), bcheck(a, 'title')) for a in iter(soup.findAll('a')) if a.has_key('href') and a['href'] and not re.match('^javascript:|^mailto:|^telnet:|^feed:', a['href']))
        except:
            return []
    
    @staticmethod
    def rss_link(url, soup):
        if isinstance(soup, (str,unicode)):
            soup = BeautifulSoup(soup, fromEncoding='utf-8')
        if not soup or not soup.head: return None
        tag = soup.head.find(name=['link'], attrs={'type':'application/rss+xml', 'rel':'alternate'})
        if not tag:
            tag = soup.head.find(name=['link'], attrs={'type':'application/atom+xml', 'rel':'alternate'})
        if not tag:
            tag = soup.find(name=['link'], attrs={'type':'application/rss+xml', 'rel':'alternate'})
        if not tag:
            tag = soup.find(name=['link'], attrs={'type':'application/atom+xml', 'rel':'alternate'})
        if not tag: return None
        if tag.has_key('href') and tag['href']: return urljoin(url, tag['href'])
        return None

def guess_baseurl(url, tree=None, retry=0):
    from SuperMario.Mario import Mario, MarioRss
    rss_url = None
    if not tree:
        mario = MarioRss()
        rss_url = mario.get_rss_url(url)
    else:
        rss_url = URL.rss_link(url, tree)
    if not rss_url: 
        baseurl = URL.baseurl(url)
        if baseurl == url or retry > 0:
            return (baseurl, None, None)
        return guess_baseurl(baseurl, None, 1)
    mario = Mario()
    while True:
        response = mario.get(rss_url, normalize=False, headers=None)
        if not response or not response.body: 
            if rss_url.endswith('/'):
                rss_url = rss_url[:-1]
                continue
            return (URL.baseurl(url), None, None)
        else:
            break
    rss = feedparser.parse(response.body)
    try:
        return (URL.normalize(rss.feed.link), rss_url, response.body)
    except:
        return (URL.baseurl(url), rss_url, response.body)

def random_sleep(minium, maxium, step=1):
    sec = random.randrange(minium, maxium, step)
    #print >>stderr, 'Sleep for %d sec.'%sec
    time.sleep(sec)
    return


def sendMail(fro, to, subject, text, files=[], server="localhost"):
    assert type(to)==list
    assert type(files)==list
    #fro = "Expediteur <expediteur@mail.com>"

    msg = MIMEMultipart()
    msg['From'] = fro
    msg['To'] = COMMASPACE.join(to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach( MIMEText(text) )

    for file in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(file,"rb").read() )
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"'
                       % os.path.basename(file))
        msg.attach(part)

    smtp = smtplib.SMTP(server)
    smtp.sendmail(fro, to, msg.as_string() )
    smtp.close()

def posterous(email, password, title, body , tags, source = "", source_link= "", site_id=None ):
    post_url = "http://posterous.com/api/newpost"
    auth = b64encode("%s:%s" % (email, password))
    if site_id:
        params = urllib.urlencode({
            "site_id" : str(site_id), # your site id
            "title" : title,
            "body" : body,
            "autopost": "1",
            "private" : "0",
            "source" : source,  
            "sourceLink" : source_link,
            "tags" : tags
        })
    else:
        params = urllib.urlencode({
            "title" : title,
            "body" : body,
            "autopost": "1",
            "private" : "0",
            "source" : source,  
            "sourceLink" : source_link,
            "tags" : tags
        }) 

    http = httplib2.Http()
    response, content = http.request(post_url, "POST",
            headers = {
                    "Content-type" : "application/x-www-form-urlencoded",
                    "Authorization" : auth }, 
            body = params )
    return response, content


def levenshtein(a,b):
    "Calculates the Levenshtein distance between a and b."
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a,b = b,a
        n,m = m,n

    current = range(n+1)
    for i in range(1,m+1):
        previous, current = current, [i]+[0]*n
        for j in range(1,n+1):
            add, delete = previous[j]+1, current[j-1]+1
            change = previous[j-1]
            if a[j-1] != b[i-1]:
                change = change + 1
            current[j] = min(add, delete, change)

    return current[n]

def levenshtein_distance(first, second):
    """Find the Levenshtein distance between two strings."""
    if len(first) > len(second):
        first, second = second, first
    if len(second) == 0:
        return len(first)
    first_length = len(first) + 1
    second_length = len(second) + 1
    distance_matrix = [range(second_length) for x in range(first_length)]
    for i in range(1, first_length):
        for j in range(1, second_length):
            deletion = distance_matrix[i-1][j] + 1
            insertion = distance_matrix[i][j-1] + 1
            substitution = distance_matrix[i-1][j-1]
            if first[i-1] != second[j-1]:
                substitution += 1
            distance_matrix[i][j] = min(insertion, deletion, substitution)

    return distance_matrix[first_length-1][second_length-1]

#Longest Common String 【最长公共字符串算法】
def lcs(first,second):
	first_length = len(first) #the first string's length
	second_length = len(second)#the second string's length
	size = 0 #length of the max string
	x = 0
	y = 0

	li = [0 for x in range(second_length)]
	for i in range(first_length):
		temp = li
		li = [0 for x in range(second_length)]
		for j in range(second_length):
			if first[i] == second[j]:
				if i - 1 >= 0 and j - 1 >=0:
					li[j] = temp[j-1] + 1 #matrix[i][j] = matrix[i-1][j-1] + 1 
				else:
					li[j] = 1
				if li[j] > size:
					size = li[j] # max length
					x = j # X-axis
					y = i # Y-axis
			else:
				li[j] = 0

	#print size,x,y
	return second[x-size+1:x+1]


class ThreadPool:

    def __init__(self,maxWorkers = 10):
        self.tasks = Queue.Queue()
        self.workers = 0
        self.working = 0
        self.maxWorkers = maxWorkers
        self.allKilled = threading.Event()
        self.countLock = threading.RLock()

        self.allKilled.set()


    def run(self, target, callback=None, *args, **kargs):
        """ starts task.
            target = callable to run with *args and **kargs arguments.
            callback = callable executed when target ends
                       callback sould accept one parameter where target's
                       return value is passed.
                       If callback is None it's ignored.
        """
        self.countLock.acquire()
        if not self.workers:
            self.addWorker()
        self.countLock.release()
        self.tasks.put((target,callback,args,kargs))


    def setMaxWorkers(self,num):
        """ Sets the maximum workers to create.
            num = max workers
                  If number passed is lower than active workers 
                  it will kill workers to match that number. 
        """
        self.countLock.acquire()
        self.maxWorkers = num
        if self.workers > self.maxWorkers:
            self.killWorker(self.workers - self.maxWorkers)
        self.countLock.release()


    def addWorker(self,num = 1):
        """ Add workers.
            num = number of workers to create/add.
        """
        for x in xrange(num):
            self.countLock.acquire()
            self.workers += 1
            self.allKilled.clear()
            self.countLock.release()        
            t = threading.Thread(target = self.__workerThread)
            t.setDaemon(True)
            t.start()


    def killWorker(self,num = 1):
        """ Kill workers.
            num = number of workers to kill.
        """
        self.countLock.acquire()
        if num > self.workers:
            num = self.workers
        self.countLock.release()
        for x in xrange(num):
            self.tasks.put("exit")            


    def killAllWorkers(self, wait=None):
        """ Kill all active workers.
            wait = seconds to wait until last worker ends
                   if None it waits forever.
        """

        self.countLock.acquire()
        self.killWorker(self.workers)
        self.countLock.release()
        self.allKilled.wait(wait)


    def __workerThread(self):
        while True:
            try:
                task = self.tasks.get(timeout=2)
            except:
                break
            # exit is "special" tasks to kill thread
            if task == "exit":
                break

            self.countLock.acquire()
            self.working += 1
            if (self.working >= self.workers) and (self.workers < self.maxWorkers): # create thread on demand
                self.addWorker()
            self.countLock.release()

            fun,cb,args,kargs = task
            try:
                ret = fun(*args,**kargs)
                if cb:
                    cb(ret)
            except Exception, err:
                print Traceback()
            self.countLock.acquire()
            self.working -= 1
            self.countLock.release()

        self.countLock.acquire()
        self.workers -= 1
        if not self.workers:
            self.allKilled.set()
        self.countLock.release()