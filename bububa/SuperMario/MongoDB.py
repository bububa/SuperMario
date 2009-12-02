#!/usr/bin/env python
# encoding: utf-8
"""
MongoDB.py

Created by Syd on 2009-08-06.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""
from datetime import datetime
from mongokit import *

class Site(MongoDocument):
    db_name = 'supermario'
    collection_name = 'sites'
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

class Page(MongoDocument):
    db_name = 'crawldb'
    collection_name = 'pages'
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

class PageVersion(MongoDocument):
    db_name = 'supermario'
    collection_name = 'page_versions'
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

class PageSandbox(MongoDocument):
    db_name = 'supermario'
    collection_name = 'sandbox'
    structure = {
        'page_versions': [unicode],
        'rss': {'url':unicode, 'body':unicode},
        'crawled_at': datetime,
        'identifier': unicode,
        'starturl': unicode,
        'analyzer': int,
        'mixed': int
    }
    indexes = [{'fields':['identifier', 'analyzer', 'mixed', 'crawled_at']}, {'fields':['analyzer', 'mixed']}]
    default_values = {'mixed':0, 'analyzer':0, 'identifier':None, 'starturl':None, 'crawled_at':datetime.utcnow, 'rss':{}}
    use_dot_notation=True

class AnalyzerCandidate(MongoDocument):
    db_name = 'supermario'
    collection_name = 'analyzer_candidates'
    structure = {
        'url_hash': unicode,
        'start_url': unicode,
        'inserted_at': datetime
    }
    indexes = [{'fields':['url_hash', 'inserted_at']}]
    default_values = {'inserted_at':datetime.utcnow}
    use_dot_notation=True
    
class Entry(MongoDocument):
    db_name = 'supermario'
    collection_name = 'entries'
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

class Image(MongoDocument):
    db_name = 'supermario'
    collection_name = 'images'
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