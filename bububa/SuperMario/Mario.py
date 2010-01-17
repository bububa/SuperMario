#!/usr/bin/env python
# encoding: utf-8
"""
Mario.py

Created by Syd on 2009-08-28.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""

import sys, os, re
import time, logging
from itertools import islice
import pycurl
import chardet
import feedparser
import random
from urlparse import urljoin
from urllib import quote
from robotparser import RobotFileParser
from eventlet.api import with_timeout
from eventlet import coros
from bububa.SuperMario.utils import Traceback, URL, ThreadPool
#from bububa.SuperMario.Storage import LightCloud

try: 
    from cStringIO import StringIO 
except ImportError: 
    from StringIO import StringIO

if os.name == 'posix': 
    # 使用pycurl.NOSIGNAL选项时忽略信号SIGPIPE 
    import signal 
    signal.signal(signal.SIGPIPE, signal.SIG_IGN) 
    del signal

_concurrent = 10
_Version = "1.2.2"

_curlinfo = (
  ("total-time", pycurl.TOTAL_TIME),
  ("upload-speed", pycurl.SPEED_UPLOAD),
  ("download-speed", pycurl.SPEED_DOWNLOAD),
)

logger = logging.getLogger("mario")
handler = logging.StreamHandler()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class LinkTitleDB:
    
    def __init__(self):
        self.dic = {}
        return
    
    def add(self, url, from_url, title, context=None):
        if url not in self.dic: self.dic[url] = []
        self.dic[url].append((from_url, title, context))
        return
    
    def dump(self):
        fp = StringIO()
        for (url, strs) in self.dic.iteritems():
            fp.write(repr((url, strs)))
            fp.write('\n')
        return fp.getvalue()


class MarioException(Exception):
    """Representation of a Mario exception."""
    def __init__(self, message, code=0):
        super(MarioException, self).__init__(message)
        self.code = code
    
    def __str__(self):
        return "%s (code=%d)" % (super(MarioException, self).__str__(), self.code)
        __repr__ = __str__


class HTTPException(MarioException):
    """An exception thrown during http(s) communication."""
    pass

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
 
# 支持的协议 
VALIDPROTOCOL = ('http', 'https', 'news', 'snews', 'nntp', 'snntp', 'ftp', 'file') 
# HTTP状态码 
STATUS_OK = (200, 203, 206) 
STATUS_ERROR = range(400, 600) 
STATUS_REDIRECT = (301, 302, 303, 307)
# 最小数据片大小(128kb) 
MINPIECESIZE = 131072 
# 最大连接数 
MAXCONCOUNT = 10 
# 最大重试数 
MAXRETRYCOUNT = 3
# 日志级别 
LOGLEVEL = logging.DEBUG 
# 清屏命令 
CLS = 'cls' if os.name == 'nt' else 'clear'
# UserAgent Definition
USER_AGENT = {'safari':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_1; en-us) AppleWebKit/531.21.8 (KHTML, like Gecko) Version/4.0.4 Safari/531.21.10', 'firefox':'Mozilla/5.0 (X11; U; Linux i686 (x86_64); de; rv:1.9.1) Gecko/20090624 Firefox/3.5'}

HEADERS = {
    'User-Agent': USER_AGENT['safari'],
    'Accept-Encoding': 'gzip,deflate',
    'Connection': 'keep-alive',
    'Keep-Alive': '300',
    'Accept': 'text/xml,application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5',
}
REFERER = 'http://boketing.com/'

class HTTPResponse(object):
    
    def __init__(self, url=None, effective_url=None, size=None, code=None, headers=None, body=None, etag=None, last_modified=None, args=None):
        if isinstance(url, unicode): url = url.encode('utf-8')
        if isinstance(effective_url, unicode): effective_url = effective_url.encode('utf-8')
        if isinstance(body, unicode): body = body.encode('utf-8')
        if isinstance(etag, unicode): etag = etag.encode('utf-8')
        if isinstance(last_modified, unicode): last_modified = last_modified.encode('utf-8')
        self.url = url
        self.effective_url = effective_url
        self.size = size
        self.code = code
        self.body = body
        self.etag = etag
        self.last_modified = last_modified
        self.args = args
    
    def __repr__(self):
        return "<%s status %s for %s>" % (self.__class__.__name__, self.code, self.effective_url.decode('utf-8'))
    
    def dump(self):
        fp = StringIO()
        fp.write(repr({'url':self.url, 'effective_url':self.effective_url, 'size':self.size, 'code':self.code, 'body':self.body}))
        return fp.getvalue()

                                         
class MarioBase(object):
    """Abstract functionality for Mario API clients.

      @type secure: bool
      @ivar secure: whether to use a secure http connection
      @type sessionId: string
      @ivar sessionId: session id from smugmug.
      @type proxy: url
      @ivar proxy: address of proxy server if one is required (http[s]://localhost[:8080])
      @type version: string
      @ivar version: which version of the SmugMug API to use
      @type verbose: function
      @ivar verbose: a function callback which takes two arguments: C{infotype} and C{message}.
      @type progress: function
      @ivar progress: a function callback which takes four arguments: C{download_total}, C{download_done},
                      C{upload_total} and C{upload_done}.
      """
    def __init__(self, callback=None, callpre=None, callfail=None, timeout=15, user_agent=None, referer = REFERER, secure=True, progress=False, proxy=False, etag=None, last_modified=None, check_duplicate=False, verbose=False, args=None):
        self.callback = callback
        self.callpre = callpre
        self.callfail = callfail
        self.timeout = timeout
        self.user_agent = user_agent
        self.referer = referer
        self.verbose = verbose
        self.progress = progress
        self.check_duplicate = check_duplicate
        self.proxy = proxy
        self.proxies = None
        self.secure = secure
        self.etag = etag
        self.last_modified = last_modified
        self.args = args
        #self.lightcloud = LightCloud.connect('n1')
    
    def connect(self, url, body=None, headers=HEADERS, normalize=True, args=None):
        url = URL.normalize(url, normalize)
        #if self.check_duplicate and URL.been_inserted(url, self.lightcloud): return None
        if callable(self.callpre): self.callpre(url)
        c = pycurl.Curl()
        if headers:
            if self.user_agent:
                headers.setdefault('User-Agent', self.user_agent)
            else:
                headers.setdefault('User-Agent', self.random_user_agent())
            header_list = []
            for header_name, header_value in headers.iteritems():
                header_list.append('%s: %s' % (header_name, header_value))
            if self.last_modified:
                header_list.append('%s: %s' % ('If-Modified-Since', self.last_modified))
            if self.etag:
                header_list.append('%s: %s' % ('ETag', self.etag))
            if header_list:
                c.setopt(pycurl.HTTPHEADER, header_list)
        #c.setopt(c.USERAGENT, self.user_agent)
        # Presence of a body indicates that we should do a POST
        if body is not None:
            logger.debug('post')
            c.setopt(pycurl.POST, 1)
            c.setopt(pycurl.POSTFIELDS, body)
        else:
            c.setopt(pycurl.HTTPGET, 1)
        c.url = url
        c.args = args
        c.setopt(pycurl.ENCODING, 'gzip, deflate')
        c.setopt(pycurl.FOLLOWLOCATION, 1) 
        c.setopt(pycurl.MAXREDIRS, 5) 
        c.setopt(pycurl.CONNECTTIMEOUT, 30) 
        c.setopt(pycurl.TIMEOUT, self.timeout) 
        c.setopt(pycurl.NOSIGNAL, 1)
        c.response = StringIO()
        c.header_data = StringIO()
        c.setopt(pycurl.WRITEFUNCTION, c.response.write)
        c.setopt(pycurl.HEADERFUNCTION, c.header_data.write)
        try:
            c.setopt(pycurl.URL, URL.quote(url))
        except:
            return None
        cookies = self.parse_cookies(c)
        if cookies:
            c.setopt(pycurl.COOKIELIST, '')
            chunks = []
            for key, value in cookies.iteritems():
                key = urllib.quote_plus(key)
                value = urllib.quote_plus(value)
                chunks.append('%s=%s;' % (key, value))
                c.setopt(pycurl.COOKIE, ''.join(chunks))
        else:
            cookie_file_name = os.tempnam()
            c.setopt(pycurl.COOKIEFILE, cookie_file_name)
            c.setopt(pycurl.COOKIEJAR, cookie_file_name)
        
        if self.referer:
            c.setopt(pycurl.REFERER, self.referer)
            
        if self.verbose:
            c.setopt(pycurl.VERBOSE, True)
            c.setopt(pycurl.DEBUGFUNCTION, self.verbose)
        
        if self.progress:
            c.setopt(pycurl.NOPROGRESS, False)
            c.setopt(pycurl.PROGRESSFUNCTION, self.progress)
        
        if self.proxies: self.proxy = random.choice(self.proxies)
        
        if self.proxy:
            if isinstance(self.proxy, (str, unicode)): proxy = self.proxy
            else: 
                proxy = self.proxy['url']
                if 'userpwd' in self.proxy:
                    c.setopt(pycurl.PROXYUSERPWD, self.proxy['proxy_userpwd'])
                if 'type' in self.proxy:
                    ptype = getattr(pycurl, 'PROXYTYPE_%s' % self.proxy['type'].upper())
                    c.setopt(pycurl.PROXYTYPE, ptype)
            c.setopt(pycurl.PROXY, proxy)
        
        if not self.secure:
            c.setopt(pycurl.SSL_VERIFYPEER, False)
            c.setopt(pycurl.SSL_VERIFYHOST, False) 
        logger.debug('connected to %r'%url)
        return c
    
    def set_referer(self, referer):
        self.referer = referer
        
    def set_proxy(self, proxy):
        self.proxy = proxy
    
    def set_proxies_list(self, proxies):
        return self.proxies
        
    def parse_cookies(self, c):
        cookies = {}
        for line in c.getinfo(pycurl.INFO_COOKIELIST):
            chunks = line.split('\t')
            cookies[chunks[-2]] = chunks[-1]
        return cookies
    
    def parse_headers(self, header):
        headers = {}
        for line in header.split('\n'):
            line = line.rstrip('\r')
            try:
                name, value = line.split(': ', 1)
                headers[name] = value
            except ValueError, err:
                continue
        return headers
                
    def random_user_agent(self):
        variants = ("Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)", "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.1)", "Mozilla/4.0 (compatible; MSIE 7.0b; Win32)", "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 6.0)", "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; SV1; Arcor 5.005; .NET CLR 1.0.3705; .NET CLR 1.1.4322)", "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; YPC 3.0.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)", "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)", "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; WOW64; SLCC1; .NET CLR 2.0.50727; .NET CLR 3.0.04506; .NET CLR 3.5.21022)", "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; WOW64; SLCC1; .NET CLR 2.0.50727; .NET CLR 3.0.04506; .NET CLR 3.5.21022)", "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; WOW64; Trident/4.0; SLCC1; .NET CLR 2.0.50727; .NET CLR 3.5.21022; .NET CLR 3.5.30729; .NET CLR 3.0.30618)", "Mozilla/5.0 (X11; U; Linux x86_64; en-US; rv:1.8.1) Gecko/20060601 Firefox/2.0 (Ubuntu-edgy)", "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.1) Gecko/20061204 Firefox/2.0.0.1", "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.1.2) Gecko/20070220 Firefox/2.0.0.2", "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.1.2) Gecko/20070221 SUSE/2.0.0.2-6.1 Firefox/2.0.0.2", "Mozilla/5.0 (Windows; U; Windows NT 5.1; en; rv:1.8.1.9) Gecko/20071025 Firefox/2.0.0.9", "Mozilla/5.0 (Windows; U; Windows NT 5.1; en; rv:1.8.1.17) Gecko/20080829 Firefox/2.0.0.17", "Mozilla/5.0 (Windows; U; Windows NT 5.1; en; rv:1.8.1.19) Gecko/20081201 Firefox/2.0.0.19", "Mozilla/5.0 (X11; U; Linux i686 (x86_64); en-US; rv:1.9a1) Gecko/20061204 GranParadiso/3.0a1", "Mozilla/5.0 (Windows; U; Windows NT 5.1; en; rv:1.9) Gecko/2008052906 Firefox/3.0", "Mozilla/5.0 (Windows; U; Windows NT 5.1; en; rv:1.9.0.1) Gecko/2008070208 Firefox/3.0.1", "Mozilla/5.0 (Windows; U; Windows NT 5.1; en; rv:1.9.0.2) Gecko/2008091620 Firefox/3.0.2", "Mozilla/5.0 (X11; U; Linux x86_64; en; rv:1.9.0.2) Gecko/2008092702 Gentoo Firefox/3.0.2", "Mozilla/5.0 (Windows; U; Windows NT 5.1; en; rv:1.9.0.3) Gecko/2008092417 Firefox/3.0.3", "Mozilla/5.0 (Windows; U; Windows NT 5.1; en; rv:1.9.0.3) Gecko/2008092417 Firefox/3.0.3 (.NET CLR 3.5.30729)", "Mozilla/5.0 (Windows; U; Windows NT 6.0; en; rv:1.9.0.3) Gecko/2008092417 Firefox/3.0.3", "Mozilla/5.0 (Windows; U; Windows NT 5.2; en; rv:1.9.0.5) Gecko/2008120122 Firefox/3.0.5", "Opera/9.0 (Windows NT 5.1; U; en)", "Opera/9.01 (X11; Linux i686; U; en)", "Opera/9.02 (Windows NT 5.1; U; en)", "Opera/9.10 (Windows NT 5.1; U; en)", "Opera/9.23 (Windows NT 5.1; U; en)", "Opera/9.50 (Windows NT 5.1; U; en)", "Opera/9.50 (Windows NT 6.0; U; en)", "Opera/9.60 (Windows NT 5.1; U; en) Presto/2.1.1")
        return random.choice(variants)
        
    def _handle_response(self, c):
        """Handle the response.
        This method decodes the response to unicode and checks for any error
        condition.  It additionally adds a C{Statistics} item to the response
        which contains upload & download times.
        
        @type c: PycURL C{Curl}
        @param c: a completed connection
        @return: a dictionary of results corresponding to the response
        @raise MarioException: if an error exists in the response
        """
        
        code = c.getinfo(c.HTTP_CODE)
        if c.errstr() == '' and c.getinfo(pycurl.RESPONSE_CODE) in STATUS_OK or code == 200:
            effective_url = c.getinfo(pycurl.EFFECTIVE_URL)
            size = int(c.getinfo(pycurl.CONTENT_LENGTH_DOWNLOAD))
        else:
            if callable(self.callfail): self.callfail(c.url)
            raise HTTPException(c.errstr(), code)
            return None
        headers = self.parse_headers(c.header_data.getvalue())
        Etag = Last_Modified = None
        if 'ETag' in headers: ETag = headers['ETag']
        if 'Last-Modified' in headers: Last_Modified = headers['Last-Modified']
                
        #if self.check_duplicate and URL.been_inserted(effective_url, self.lightcloud): return None
        body = c.response.getvalue()
        try:
            charset = chardet.detect(body)
            if charset and charset['encoding'] and charset['encoding'].lower()!='utf-8' and charset['encoding'].lower()!='iso-8859-2':
                charset['encoding'] = charset['encoding'].lower()
                if charset['encoding'] in ALT_CODECS: charset['encoding'] = ALT_CODECS[charset['encoding']]
                encoding = charset['encoding']
                body = body.decode(encoding).encode('utf-8')
            elif charset and charset['encoding'] and charset['encoding'].lower()!='iso-8859-2':
                pattern = re.compile('<meta http-equiv="Content-Type" content="text/html; charset=([^^].*?)"', re.I|re.S)
                encoding = pattern.findall(body)
                if encoding:
                    encoding = encoding[0].lower()
                    if encoding in ALT_CODECS: encoding = ALT_CODECS[encoding]
                    if encoding.lower()!='iso-8859-2' and encoding.lower()!='utf-8':
                    body = body.decode(encoding).encode('utf-8')
        except UnicodeDecodeError, err:
            body = body.decode(encoding, "replace").encode('utf-8')
            #if callable(self.callfail): self.callfail(effective_url)
            #logger.error('Encoding error: %r'%c.url)
            #logger.error(err)
            #return None
        response = HTTPResponse(url=c.url, effective_url=URL.normalize(effective_url), size=size, code=code, body=body, etag = Etag, last_modified = Last_Modified, args=c.args)
        logger.debug(response)
        try:
            if callable(self.callback): self.callback(response)
            return response
        except:
            if callable(self.callfail): self.callfail(effective_url)
            logger.error('Error: %r'%Traceback())
            return None
    
    def _handle_response_header(self, c):
        """Handle the response.
        This method decodes the response to unicode and checks for any error
        condition.  It additionally adds a C{Statistics} item to the response
        which contains upload & download times.

        @type c: PycURL C{Curl}
        @param c: a completed connection
        @return: a dictionary of results corresponding to the response
        @raise MarioException: if an error exists in the response
        """

        code = c.getinfo(c.HTTP_CODE)
        if c.errstr() == '' and c.getinfo(pycurl.RESPONSE_CODE) in STATUS_OK or code == 200:
            effective_url = c.getinfo(pycurl.EFFECTIVE_URL)
            size = int(c.getinfo(pycurl.CONTENT_LENGTH_DOWNLOAD))
        else:
            if callable(self.callfail): self.callfail(c.url)
            raise HTTPException(c.errstr(), code)
            return None
        #if self.check_duplicate and URL.been_inserted(effective_url, self.lightcloud): return None
        return URL.normalize(effective_url)
    
    def _perform(self, c):
        """Execute the request.
        
        A request pending execution.
        
        @type c: PycURL C{Curl}
        @param c: a pending request
        """
        pass
            
    def close(self, c):
        c.close()
    
    def batch(self):
        """Return an instance of a batch-oriented Mario client."""
        return MarioBatch(secure=self.secure, proxy=self.proxy, verbose=self.verbose, progress=self.progress)


class Mario(MarioBase):
    
    def get(self, url, normalize=True, body=None, headers=HEADERS, referer=None, etag=None, last_modified=None, proxy=None):
        self.url = url
        if referer: self.set_referer(referer)
        if proxy: self.set_proxy(proxy)
        if etag: self.etag = etag
        if last_modified: self.last_modified = last_modified
        c = self.connect(url=url, normalize=normalize, body=body, headers=headers)
        return self._perform(c)
    
    def effective_url(self, url, normalize=True, body=None, headers=HEADERS):
        self.url = url
        c = self.connect(url=url, normalize=normalize, body=body, headers=headers)
        return self._effective_url(c)
        
    def _perform(self, c):
        """Perform the low-level communication with Mario."""
        try:
            c.perform()
            return self._handle_response(c)
        except HTTPException:
            if self.url.endswith('/'):
                return self.get(self.url[:-1], False)
            return None
        except:
            logger.error(Traceback())
            return None
        finally:
            c.close()
    
    def _effective_url(self, c):
        """Perform the low-level communication with Mario."""
        try:
            c.perform()
            return self._handle_response_header(c)
        except HTTPException:
            if self.url.endswith('/'):
                return self.effective_url(self.url[:-1], False)
            return None
        except:
            logger.error(Traceback())
            return None
        finally:
            c.close()


class MarioBatch(MarioBase):
    """Batching version of a Mario client.
        
    @type _batch: list<PycURL C{Curl}>
    @ivar _batch: list of requests pending executions
    @type concurrent: int
    @ivar concurrent: number of concurrent requests to execute
    """
    def __init__(self, *args, **kwargs):
        concurrent = kwargs.pop("concurrent", _concurrent)
        super(MarioBatch, self).__init__(*args, **kwargs)
        self._batch = list()
        self.concurrent = concurrent
    
    def add_job(self, url, body=None, headers=HEADERS, args=None):
        c = self.connect(url, body, headers, True, args)
        self._perform(c)
    
    def _perform(self, c):
        """Store the request for later processing."""
        if c: self._batch.append(c)
        return None
    
    def __len__(self):
        return len(self._batch)
    
    def __call__(self, n=None):
        """Execute all pending requests.
        @type n: int
        @param n: maximum number of simultaneous connections
        @return: a generator of results from the batch execution - order independent
        """
        try:
            return self._multi(self._batch[:], self._handle_response, n=n)
        except:
            logger.error(Traceback())
        finally:
            self._batch = list()
    
    def _handle_response(self, c):
        """Catch any exceptions and return a valid response.  The default behaviour
        is to raise the exception immediately but in a batch environment this is not
        acceptable.
        
        @type c: PycURL C{Curl}
        @param c: a completed connection
        """
        try:
            return super(MarioBatch, self)._handle_response(c)
        except Exception, e:
            logger.debug('Error: %r'%Traceback())
            return {"exception":e, "stat":"fail", "code":-1}
        finally:
            c.close()
    
    def _multi(self, batch, func, n=None):
        """Perform the concurrent execution of all pending requests.
        
        This method iterates over all the outstanding working at most
        C{n} concurrently.  On completion of each request the callback
        function C{func} is invoked with the completed PycURL instance
        from which the C{params} and C{response} can be extracted.
        
        There is no distinction between a failure or success reponse,
        both are C{yield}ed.
        
        After receiving I{all} responses, the requests are closed.
        
        @type batch: list<PycURL C{Curl}>
        @param batch: a list of pending requests
        @param func: callback function invoked on each completed request
        @type n: int
        @param n: the number of concurrent events to execute
        """
        if not batch:
            raise StopIteration()
        
        n = (n if n is not None else self.concurrent)
        if n <= 0:
            raise MarioException("concurrent requests must be greater than zero")
        logger.debug("using %d concurrent connections", n)
        
        ibatch = iter(batch)
        total, working = len(batch), 0
        m = pycurl.CurlMulti()
        while total > 0:
            for c in islice(ibatch, (n-working)):
                if not c: continue
                m.add_handle(c)
                working += 1
            while True:
                ret, nhandles = m.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break
            while True:
                q, ok, err = m.info_read()
                for c in ok:
                    m.remove_handle(c)
                    func(c)
                    #yield (c.args, func(c))
                for c, errno, errmsg in err:
                    m.remove_handle(c)
                    func(c)
                    #yield (c.args, func(c))
                read = len(ok) + len(err)
                total -= read
                working -= read
                if q == 0:
                    break
            m.select(1.0)
        
        while batch:
            try:
                batch.pop().close()
            except:
                logger.debug('Error: %r'%Traceback())


class MarioThread:
    def __init__(self, callback=None):
        self.urls = []
        self.callback = callback
    
    def __call__(self, concurrent=None):
        total_workers = len(self.urls)
        for i in xrange(0, total_workers, concurrent):
            self.run_workers(self.urls[i:i + concurrent])
    
    def add_job(self, url):
        self.urls.append(url)
        
    def run_workers(self, urls):
        threadPool = ThreadPool(len(urls))
        for url in urls:
            threadPool.run(self.run_mario, callback=self.callback, url=url)
        threadPool.killAllWorkers(None)
    
    def run_mario(self, url):
        mario = Mario()
        return mario.get(url)


class MarioRss:
    def __init__(self, callback=None, callpre=None, callfail=None, concount=MAXCONCOUNT, check_duplicate=False, mutilthread=False):
        self.concount = concount
        self.callback = callback
        self.callpre = callpre
        self.callfail = callfail
        self.check_duplicate = check_duplicate
        self.mutilthread = mutilthread
        self.link_title_db = LinkTitleDB()
    
    def get(self, starturl, rssurl=None, rssBody=None, etag=None, last_modified=None, limit=None, proxy=None):
        if not rssurl: rssurl = self.get_rss_url(starturl, etag=etag, last_modified=last_modified, proxy=proxy)
        elif not rssurl.startswith('http://feeds.feedburner.com'): 
            mario = Mario(etag=etag, last_modified=lastmodified, proxy=proxy)
            rssurl = mario.effective_url(rssurl)
        if not rssurl:
            logger.debug("Didn't find rss feed for %s"%starturl)
            return None
        rssEtag = rssLastModified = None
        if not rssBody:
            mario = Mario()
            if rssurl.startswith('http://feeds.feedburner.com'): response = mario.get(rssurl, headers=None)
            else: response = mario.get(rssurl)
            if not response:
                logger.debug("Can't fetch rss feed at %s"%rssurl)
                return None
            rssBody = response.body
            rssEtag = response.etag
            rssLastModified = response.last_modified
        rss = feedparser.parse(rssBody)
        if not rss['entries']: return None
        if limit: rss['entries'] = rss['entries'][:limit]
        if not self.callback:
            return {'url': rssurl, 'effective_url': rssurl, 'body': rssBody, 'code':'200', 'size':len(rssBody), 'etag':rssEtag, 'last_modified':rssLastModified}
        if self.multithread:
            mario = MarioThread(self.callback)
            for entry in rss['entries']:
                mario.add_job(entry['link'])
                self.link_title_db.add(entry['link'], '', entry['title'], entry)
            mario(self.concount)
        else:
            mario = MarioBatch(callback=self.callback, callpre=self.callpre, callfail=self.callfail, check_duplicate=self.check_duplicate, referer=rssurl, proxy=proxy)
            pool = coros.CoroutinePool(max_size=len(rss['entries']))
            waiters = []
            for entry in rss['entries']:
                #self.add_job(mario, entry)
                #if self.check_duplicate and URL.been_inserted(URL.normalize(entry['links']), mario.lightcloud): 
                #    logger.debug('Has been inserted. %r'%entry['link'])
                #    continue
                waiters.append(pool.execute(self.add_job, mario, entry))
            for waiter in waiters:
                waiter.wait()
            mario(self.concount)
        return {'url': rssurl, 'effective_url': rssurl, 'body': rssBody, 'code':'200', 'size':len(rssBody), 'etag':rssEtag, 'last_modified':rssLastModified}
    
    def add_job(self, mario, entry):
        mario.add_job(entry['link'])
        self.link_title_db.add(entry['link'], '', entry['title'], entry)
        return
        
    def get_rss_url(self, starturl, etag=None, last_modified=None, proxy=None):
        mario = Mario(referer=starturl, etag=etag, last_modified=last_modified, proxy=proxy)
        response = mario.get(starturl)
        if not response: return None
        return URL.rss_link(starturl, response.body)


class MarioDepth:
    def __init__(self, starturl, callback, callpre=None, callfail=None, concount=MAXCONCOUNT, depth=2, accept_url_patterns=None, reject_url_patterns=None):
        self.concount = concount
        self.callback = callback
        self.callpre = callpre
        self.callfail = callfail
        self.depth = depth
        self.starturl = starturl
        self.baseurl = URL.baseurl(starturl)
        self.urls = []
        self.crawled = {}
        self.link_title_db = LinkTitleDB()
        self.accept_url_patterns = accept_url_patterns
        self.reject_url_patterns = reject_url_patterns
        self.robotstxt = RobotFileParser()
        self.robotstxt.set_url(urljoin(starturl, '/robots.txt'))
        self.referer = starturl
        try:
            self.robotstxt.read()
        except:
            logger.debug(Traceback())
        #self.lightcloud = LightCloud.connect('n0')
    
    def __call__(self, n=None):
        if n: self.concount = n
        current_depth = self.depth
        self.urls.append((self.starturl, current_depth))
        while self.urls:
            self.depth_get()
            logger.debug('%d unprocessed urls'%(len(self.urls)))
    
    def depth_get(self):
        mario = MarioBatch(callback=self.next_depth, callpre=self.callpre, callfail=self.callfail)
        pool = coros.CoroutinePool(max_size=len(self.urls))
        while self.urls:
            waiters = []
            #self.add_job(mario)
            counter = 0
            while self.urls:
                if counter > 9: break;
                counter += 1
                waiters.append(pool.execute(self.add_job, mario))
            logger.debug('Depth break')
            for waiter in waiters:
                waiter.wait()
            mario(self.concount)
    
    def add_job(self, mario):
        if not self.urls: return
        url, depth = self.urls.pop()
        if self.visited(url, depth): return
        mario.add_job(url, args=depth)
        
    def visited(self, url, depth):
        #is_duplicate = URL.is_duplicate(url, self.lightcloud)
        return depth==0 and is_duplicate or depth < self.depth and self.crawled.has_key(url) and self.crawled[url] == 2
    
    def next_depth(self, response):
        #with_timeout(1, self.lightcloud.set, LightCloud.crawled_url_key(response.effective_url), response.url, timeout_value=None)
        for link, title in URL.link_title(response.body, response.effective_url):
            if not self.inject_url(link, response.args):continue
            self.link_title_db.add(link, response.effective_url, title)
        if callable(self.callback): self.callback(response)
        self.crawled[response.effective_url] = 2
        if response.effective_url != response.url:
            self.crawled[response.url] = 2
        self.referer = response.effective_url
    
    def inject_url(self, url, depth):
        if not (depth and url and url not in self.crawled): 
            #logger.debug('IGNORE(%d): %r'%(depth, url))
            return None
        if isinstance(url, unicode): url = url.encode('utf-8')
        if self.reject_url(url): 
            logger.debug('REJECT: %r' % url)
            return None
        try:
            can_fetch = self.robotstxt.can_fetch(USER_AGENT['safari'], url)
        except:
            can_fetch = True
        if self.baseurl!='http://hi.baidu.com/' and not can_fetch:
            logger.debug('DISALLOW: %r' % url)
            return None
        logger.debug('INJECT(%d): %r' % (depth-1, url))
        self.crawled[url] = 1
        self.urls.append((url, depth-1))
        return True
    
    def reject_url(self, url):
        return self.baseurl != URL.baseurl(url) and (not self.accept_url_patterns or not re.match('|'.join(self.accept_url_patterns), url) or self.reject_url_patterns or re.match('|'.join(self.reject_url_patterns), url))
        