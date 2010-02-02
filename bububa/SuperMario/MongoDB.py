#!/usr/bin/env python
# encoding: utf-8
"""
MongoDB.py

Created by Syd on 2009-08-06.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""
from datetime import datetime
from mongokit import *

MongoHost = 'localhost'
MongoPort = '27017'

class SiteDocument(Document):
    structure = {
        'url': unicode,
        'url_hash': unicode,
        'rss_url': unicode,
        'sp': unicode,
        'update_freq': float,
        'inserted_at': datetime,
        'last_updated_at': datetime,
        'rank': int,
        'pattern': unicode
    }
    required_fields = ['url', 'url_hash', 'inserted_at']
    indexes = [{'fields':'url_hash', 'unique':True}]
    default_values = {'update_freq':0.0, 'rank':0}
    use_dot_notation=True

class PageDocument(Document):
    structure = {
        'url': unicode,
        'url_hash': unicode,
        'site': unicode,
        'failed_freq': float,
        'update_freq': float,
        'rank': int,
        'anchors': [{'name':unicode, 'url':unicode}]
    }
    required_fields = ['url', 'url_hash']
    indexes = [{'fields':'url_hash', 'unique':True}, {'fields':'site'}, {'fields':'url_hash'}]
    default_values = {'failed_freq':0.0, 'update_freq':0.0, 'rank':0, 'anchors':[]}
    use_dot_notation=True

class PageVersionDocument(Document):
    structure = {
        'crawled_at': datetime,
        'raw': unicode,
        'content_hash': unicode,
        'url': unicode,
        'page': unicode,
        'code': int
    }
    indexes = [{'fields':'page', 'unique':True}, {'fields':'crawled_at'}]
    default_values = {'crawled_at':datetime.utcnow}
    use_dot_notation=True

class PageSandboxDocument(Document):
    structure = {
        'page_versions': [unicode],
        'rss': {'url':unicode, 'body':unicode},
        'crawled_at': datetime,
        'identifier': unicode,
        'starturl': unicode,
        'analyzer': int,
        'mixed': int
    }
    indexes = [{'fields':[('identifier', INDEX_ASCENDING), ('analyzer', INDEX_ASCENDING), ('mixed', INDEX_ASCENDING), ('crawled_at', INDEX_ASCENDING)]}, {'fields':[('analyzer', INDEX_ASCENDING), ('mixed', INDEX_ASCENDING)]}]
    default_values = {'mixed':0, 'analyzer':0, 'identifier':None, 'starturl':None, 'crawled_at':datetime.utcnow, 'rss':{}}
    use_dot_notation=True

class AnalyzerCandidateDocument(Document):
    structure = {
        'url_hash': unicode,
        'start_url': unicode,
        'inserted_at': datetime
    }
    indexes = [{'fields':[('url_hash', INDEX_ASCENDING), ('inserted_at', INDEX_ASCENDING)]}]
    default_values = {'inserted_at':datetime.utcnow}
    use_dot_notation=True
    
class EntryDocument(Document):
    structure = {
        'url': unicode, 
        'url_hash': unicode,
        'title': unicode,
        'content': unicode,
        'author': unicode,
        'tags': [unicode],
        'published_at': datetime,
        'updated_at': datetime,
        'identifier': unicode
    }
    indexes = [{'fields':'url_hash', 'unique':True}, {'fields':'identifier'}]
    default_values = {'published_at':datetime.utcnow, 'updated_at':datetime.utcnow}
    use_dot_notation=True

class ImageDocument(Document):
    structure = {
        'url': unicode,
        'meta': unicode,
        'title': unicode,
        'date': unicode,
        'keyword': unicode,
        'inserted_at': datetime
    }
    default_values = {'inserted_at': datetime.utcnow}
    use_dot_notation=True


def reconnect():
    try:
        conn = Connection(MongoHost, MongoPort)
    except AutoReconnect, err:
        conn = None
    return conn


def Site(conn=None):
    if not conn: conn = reconnect()
    if not conn: return None
    conn.register([SiteDocument])
    return conn.supermario.sites.SiteDocument()


def Page(conn=None):
    if not conn: conn = reconnect()
    if not conn: return None
    conn.register([PageDocument])
    return conn.crawldb.pages.PageDocument()


def PageVersion(conn=None):
    if not conn: conn = reconnect()
    if not conn: return None
    conn.register([PageVersionDocument])
    return conn.supermario.pageversions.PageVersionDocument()


def PageSandbox(conn=None):
    if not conn: conn = reconnect()
    if not conn: return None
    conn.register([PageSandboxDocument])
    return conn.supermario.sandbox.PageSandboxDocument()


def AnalyzerCandidate(conn=None):
    if not conn: conn = reconnect()
    if not conn: return None
    conn.register([AnalyzerCandidateDocument])
    return conn.supermario.analyzer_candidates.AnalyzerCandidateDocument()


def Entry(conn=None):
    if not conn: conn = reconnect()
    if not conn: return None
    conn.register([EntryDocument])
    return conn.supermario.entries.EntryDocument()


def Image(conn=None):
    if not conn: conn = reconnect()
    if not conn: return None
    conn.register([ImageDocument])
    return conn.supermario.images.ImageDocument()

def New(obj):
    return obj()