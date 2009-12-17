#!/usr/bin/env python
# encoding: utf-8
"""
Warehouse.py

Created by Syd on 2009-08-28.
Copyright (c) 2009 __MyCompanyName__. All rights reserved.
"""

import sys
import os
import shutil
import re
import time
import logging
from hashlib import md5, sha256
from datetime import datetime, timedelta
try:
    import cPickle as pickle
except:
    import pickle
from BeautifulSoup import BeautifulSoup
import feedparser
try:
    from mongokit import *
    from MongoDB import Site, Page, PageVersion, PageSandbox, Image
except:
    pass
from eventlet import db_pool
from eventlet.db_pool import ConnectTimeout
from MySQLdb.cursors import DictCursor
from bububa.SuperMario.Mario import Mario, MarioDepth, MarioRss, MarioBatch, MarioThread
from bububa.SuperMario.utils import URL, Traceback, ThreadPool
from bububa.SuperMario.Storage import DatabaseConnector, ConnectionPool
from bububa.SuperMario.bsp import BSP
from bububa.SuperMario.CoreImage import CoreImage

logger = logging.getLogger("warehouse")
handler = logging.StreamHandler()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

CRAWLER_TIME_DELTA = timedelta(hours=1)

CONCOUNT = 10
try:
    CONNECTION = Connection()
except:
    logger.debug('Can not connect to mongodb')


class WarehouseBase(object):
    def __init__(self, starturl, identifier, accept_url_patterns=[], reject_url_patterns=[], analysis=False, verbose=False):
        self.starturl = starturl
        self.identifier = identifier
        self.analysis = analysis
        self.results = []
    
    def check_duplicate_sandbox(self, url):
        sandboxes = PageSandbox.all()
        for sandbox in sandboxes:
            for version in sandbox.page_versions:
                page_version = PageVersion.one({'_id':version})
                if page_version.url == url:
                    return True
        return None
        
    def dump(self, mario, rss=None):
        page_sandbox = PageSandbox()
        if self.analysis: page_sandbox.analyzer = 1
        if self.mixed: page_sandbox.mixed = 1
        page_sandbox.identifier = self.identifier if isinstance(self.identifier, unicode) else self.identifier.decode('utf-8')
        page_sandbox.starturl = self.starturl if isinstance(self.starturl, unicode) else self.starturl.decode('utf-8')
        for r in self.results:
            url_hash = md5(r.effective_url).hexdigest()
            if not self.analysis and self.check_duplicate_sandbox(r.url if isinstance(r.url, unicode) else r.url.decode('utf-8')):
                logger.debug("Existed in sandbox: %s"%(r.url))
                continue
            page = Page.one({'url_hash':url_hash})
            if not page:
                page = Page()
                page.effective_url = r.effective_url if isinstance(r.effective_url, unicode) else r.effective_url.decode('utf-8')
                page.url_hash = url_hash if isinstance(url_hash, unicode) else url_hash.decode('utf-8')
            for links in (links for url, links in mario.link_title_db.dic.items() if url == r.effective_url):
                for link, title, context in links:
                    lt = {'name':title if isinstance(title, unicode) else title.decode('utf-8'), 'url':link if isinstance(link, unicode) else link.decode('utf-8')}
                    if lt not in page.anchors:
                        page.anchors.append(lt)
            page.save()
            page_versions = PageVersion.all({'page':page._id}).limit(1)
            if page_versions.count() > 0:
                for page_version in page_versions:
                    page_sandbox.page_versions.append(page_version._id)
                    break
            if not page_versions:
                page_version = PageVersion()
                page_version.page= page._id
                page_version.code = r.code
                body = r.body if isinstance(r.body, unicode) else r.body.decode('utf-8')
                page_version.raw = body
                page_version.content_hash = sha256(body.encode('utf-8')).hexdigest().decode('utf-8')
                page_version.url = r.url if isinstance(r.url, unicode) else r.url.decode('utf-8')
                page_version.save()
                page_sandbox.page_versions.append(page_version._id)
        if rss:
            try:
                page_sandbox.rss.url = rss[0] if isinstance(rss[0], unicode) else rss[0].decode('utf-8')
                page_sandbox.rss.body = rss[1] if isinstance(rss[1], unicode) else rss[1].decode('utf-8')
            except:
                pass
        page_sandbox.save()

    def callback(self, response):
        self.cache_url(response.effective_url, 1, response)
        #if response.effective_url != response.url:
        #    self.cache_url(response.url, 1)
        self.results.append(response)

    def callpre(self, url):
        self.cache_url(url, 0)

    def callfail(self, url):
        self.cache_url(url, 2)

    def cache_url(self, url, status, response=None):
        url_hash = md5(url).hexdigest()
        page = Page.one({'url_hash':url_hash})
        if not page:
            page = Page()
            page.url = url if isinstance(url, unicode) else url.decode('utf-8')
            page.url_hash = url_hash if isinstance(url_hash, unicode) else url_hash.decode('utf-8')
        page.save()
        if not response: return
        page_version = PageVersion()
        page_version.page= page._id
        page_version.code = response.code
        body = response.body if isinstance(response.body, unicode) else response.body.decode('utf-8')
        page_version.raw = body
        page_version.content_hash = sha256(body.encode('utf-8')).hexdigest().decode('utf-8')
        page_version.url = response.url if isinstance(response.url, unicode) else response.url.decode('utf-8')
        page_version.save()
        return


class Warehouse(WarehouseBase):
    
    def __init__(self, starturl, identifier=None, accept_url_patterns=[], reject_url_patterns=[], analysis=False, verbose=False):
        starturl = URL.normalize(starturl)
        self.analysis = analysis
        self.mixed = 0
        if not identifier: identifier = md5(starturl).hexdigest()
        super(Warehouse, self).__init__(starturl, identifier=identifier, accept_url_patterns=accept_url_patterns, reject_url_patterns=reject_url_patterns, analysis=analysis, verbose=verbose)
        bsp = BSP()
        bsp_pac = bsp.get_pac(starturl)
        pac = None
        if bsp_pac:
            pac = bsp_pac
        if not Site.one({"url_hash": identifier}):
            site = Site()
            site.url = starturl if isinstance(starturl, unicode) else starturl.decode('utf-8')
            site.url_hash = identifier if isinstance(identifier, unicode) else identifier.decode('utf-8')
            site.inserted_at = datetime.utcnow()
            site.last_updated_at = datetime.utcnow()
            site.save()
    
    def runDepth(self, depth=1, concount=CONCOUNT):
        mario = MarioDepth(starturl=self.starturl, depth=depth, callback=self.callback, callpre=self.callpre, callfail=self.callfail)
        mario(concount)
        self.dump(mario)
    
    def runRss(self, rssurl=None, rssBody=None, concount=CONCOUNT):
        if self.analysis: limit = 10
        else: limit = None
        mario = MarioRss(callback=self.callback, callpre=self.callpre, callfail=self.callfail, concount=concount)
        rss = mario.get(self.starturl, rssurl, rssBody, limit)
        self.dump(mario, rss)


class WarehouseRss(WarehouseBase):
    def __init__(self, starturl, identifier=None, verbose=False):
        starturl = URL.normalize(starturl)
        self.mixed = 1
        if not identifier: identifier = md5(starturl).hexdigest()
        super(WarehouseRss, self).__init__(starturl, identifier=identifier, verbose=verbose)
    
    def run(self, rssurl=None, rssbody=None, concount=CONCOUNT):
        mario = MarioRss(callback=self.callback, callpre=self.callpre, callfail=self.callfail, check_duplicate=True, concount=concount, multithread=True)
        rss = mario.get(self.starturl, rssurl, rssbody)
        self.dump(mario, rss)


class WarehouseImage:
    
    def __init__(self, keyword, mark_path=None):
        self.keyword = keyword
        self.mark_path = mark_path
        self.urls = []
    
    def download(self, image_base_path, image_ori_path, image_512_path, image_64_path, concount=CONCOUNT):
        if not self.urls: 
            logger.debug('No images need to be download')
            return
        for p in (image_base_path, image_ori_path, image_512_path, image_64_path):
            if not os.path.isdir(p): os.mkdir(p)
        mario = MarioBatch(callback=self.download_callback)
        for url, meta in self.urls:
            if Image.one({'url': url}): continue
            mario.add_job(url)
        mario(concount)
    
    def download_callback(self, response):
        meta = self.get_meta(response.effective_url)
        if self.keyword not in meta: meta = '%s %s'%(self.keyword, meta)
        meta.strip()
        now = datetime.now()
        url_title = '%s-%d'%('-'.join(meta.split(' ')[:5]), time.mktime(datetime.timetuple(now)))
        date = time.strftime("%Y%m%d", datetime.timetuple(now))
        date_path = os.path.join(image_ori_path, date)
        if not os.path.isdir(date_path): os.mkdir(date_path)
        ori_file = '%s.jpg'%os.path.join( date_path, url_title)
        fp = file(ori_file, 'wb')
        fp.write(response.body)
        fp.close()
        if self.mark_path:
            mark = CoreImage.open(self.mark_path)
        else:
            mark = None
        date_path = os.path.join(image_512_path, date)
        if not os.path.isdir(date_path): os.mkdir(date_path)
        CoreImage.thumbnail(ori_file, '%s.png'%os.path.join(date_path, url_title), 'PNG', 512, mark, 100)
        date_path = os.path.join(image_64_path, date)
        if not os.path.isdir(date_path): os.mkdir(date_path)
        CoreImage.thumbnail(ori_file, '%s.png'%os.path.join(date_path, url_title), 'PNG', 64)
        self.save(response.effective_url, meta, date, url_title)
    
    def get_meta(self, u):
        for url, meta in self.urls:
            if url == u: return meta.lower()
        return self.keyword
        
    def save(self, url, meta, date, title):
        imdb = Image()
        imdb.url = url if isinstance(url, unicode) else url.decode('utf-8')
        imdb.meta = meta if isinstance(meta, unicode) else meta.decode('utf-8')
        imdb.date = date if isinstance(date, unicode) else date.decode('utf-8')
        imdb.title = title if isinstance(title, unicode) else title.decode('utf-8')
        imdb.keyword = self.keyword if isinstance(self.keyword, unicode) else self.keyword.decode('utf-8')
        imdb.save()
        return
        
    def flickr(self, flickr_api_key, depth=5):
        api_key = flickr_api_key
        total_pages = depth
        url_form = 'http://%(farm_id)s.static.flickr.com/%(server_id)s/%(id)s_%(secret)s_b.jpg'
        flickr = flickrapi.FlickrAPI(api_key)
        cur_page = 1
        while cur_page <= depth and cur_page <= total_pages:
            try:
                rsp = flickr.photos_search(text=self.keyword, media='photos', per_page='10', page=cur_page)
            except:
                total_pages=0
                logger.error(Traceback())
                continue
            total_pages = rsp[0].attrib['pages']
            photos = rsp.find('photos')
            for photo in photos:
                self.urls.append( (URL.normalize(url_form%{'farm_id':photo.attrib['farm'], 'server_id':photo.attrib['server'], 'id':photo.attrib['id'], 'secret':photo.attrib['secret']}), photo.attrib['title']) )
            cur_page += 1
        
    
    def photobucket(self, concount=CONCOUNT):
        base_url = 'http://www.photobucket.com/images/%s/'%self.keyword
        rss_url = 'http://feed.photobucket.com/images/%s/feed.rss'%self.keyword
        mario = Mario(callback=self.photobucket_callback)
        mario.get(rss_url)
    
    def photobucket_callback(self, response):
        try:
            rss = feedparser.parse(response.body)
        except:
            return
        for entry in rss['entries']:
            self.urls.append( (URL.normalize(entry.guid), entry.title) )


class WarehouseProxy:
    def __init__(self, filename):
        self.concount = 10
        self.proxies = []
        self.filename = filename
        self.urls = ('http://www.atomintersoft.com/products/alive-proxy/proxy-list', 'http://www.atomintersoft.com/proxy_list_China_cn', 'http://www.1flat.com/proxy_list.js', 'http://www.chinapromoter.com/proxy/index.htm', 'http://www.cnproxy.com/proxy1.html', 'http://www.cnproxy.com/proxy2.html', 'http://www.cnproxy.com/proxy3.html', 'http://www.cnproxy.com/proxy4.html', 'http://www.cnproxy.com/proxy5.html', 'http://www.cnproxy.com/proxy6.html', 'http://www.66dl.com/ssgx.html')

    def get_proxies(self, urls=None):
        if not urls: urls = self.urls
        mario = MarioThread(callback=self.extract_proxies)
        for url in urls:
            mario.add_job(url)
        mario(self.concount)
        self.save_proxies(self.proxies)

    def extract_proxies(self, response):
        if not response or not response.body: return
        if '1flat.com' in response.url:
            pattern = re.compile("\['(.*?)',.*?, .*?, (\d+),'.*?'\]")
            res = pattern.findall(response.body)
            if not res:return
            self.proxies.extend('%s:%s'%(r[0], r[1]) for r in res if r)
        elif 'cnproxy.com' in response.url:
            pattern = re.compile('<td>(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s*?<SCRIPT type=text/javascript>document.write\(":"(.*?)\)</SCRIPT></td>')
            res = pattern.findall(response.body)
            if not res: return
            rep = {'\+':'', 'z':"3",'m':"4",'k':"2",'l':"9",'d':"0",'x':"5",'i':"7",'w':"6",'q':"8",'b':"1"}
            for r in res:
                port = r[1]
                if not r or len(r)!=2: continue
                for k, v in rep.items():
                    port = re.sub(k, v, port)
                self.proxies.append('%s:%s'%(r[0], port))
        elif 'chinapromoter.com' in response.url:
            pattern = re.compile('<TD.*?>(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s*?</TD>\s*?<TD>(\d{2,5})\s*?</TD>')
            res = pattern.findall(response.body)
            if not res:return
            self.proxies.extend('%s:%s'%(r[0], r[1]) for r in res if r)
        else:
            pattern = re.compile('((\d{1,3}\.){3}\d{1,3}:\d{2,5})')
            res = pattern.findall(response.body)
            if not res: return
            self.proxies.extend([r[0] for r in res if r])

    def save_proxies(self, proxies, truncate=False):
        if not proxies: return
        if not truncate:
            try:
                fp = file(self.filename, 'r')
                proxies.extend([line.strip() for line in fp.readlines() if line.strip() and line.strip() not in proxies])
                fp.close()
            except IOError:
                pass
        self.proxies = []
        max_chunk = 30
        total_workers = len(proxies)
        for i in xrange(0, total_workers, max_chunk):
            self.run_workers(proxies[i:i + max_chunk])
        if not self.proxies: return
        fp = file(self.filename, 'w')
        fp.writelines(['%s\n'%p for p in self.proxies])
        fp.close()

    def run_workers(self, proxies):
        threadPool = ThreadPool(len(proxies))
        for proxy in proxies:
            threadPool.run(self.accept_proxy, callback=None, proxy=proxy)
        threadPool.killAllWorkers(None)

    def accept_proxy(self, proxy):
        if self.check_proxy(proxy):
            self.proxies.append(proxy)

    def read_proxies(self):
        try:
            fp = file(self.filename, 'r')
            proxies = [line.strip() for line in fp.readlines() if line.strip()]
            fp.close()
        except IOError:
            return None
        return proxies

    def check_proxy(self, proxy):
        if not proxy: return None
        url = 'http://www.baidu.com'
        mario = Mario()
        logger.debug('proxy: %s'%proxy)
        res = mario.get(url=url, proxy={'url': proxy})
        return res

    def check_proxies(self, proxies=None):
        if not proxies: proxies = self.read_proxies()
        if not proxies: return None
        self.save_proxies(proxies, True)
