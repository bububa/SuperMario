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
from mongokit import *
from MongoDB import Site, Page, PageVersion, PageSandbox, Image
from eventlet import db_pool
from eventlet.db_pool import ConnectTimeout
from MySQLdb.cursors import DictCursor
from bububa.SuperMario.Mario import Mario, MarioDepth, MarioRss, MarioBatch
from bububa.SuperMario.utils import URL, Traceback
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
        mario = MarioRss(callback=self.callback, callpre=self.callpre, callfail=self.callfail, check_duplicate=True, concount=concount)
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
            