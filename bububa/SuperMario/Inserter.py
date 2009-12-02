#!/usr/bin/env python
# encoding: utf-8
"""
Inserter.py

Created by Syd on 2009-08-20.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""

import sys
import os
import shutil
import time
from time import strftime
from hashlib import md5
import simplejson
import logging
from datetime import datetime, timedelta
from MySQLdb.cursors import DictCursor
from bububa.SuperMario.Storage import DatabaseConnector, ConnectionPool
from bububa.eventlet.db_pool import ConnectTimeout
from bububa.SuperMario.utils import Traceback
from bububa.SuperMario.MongoDB import *

logger = logging.getLogger("Inserter")
handler = logging.StreamHandler()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

stderr = sys.stderr

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


class BaseInserter:
    
    def __init__(self, db_pool_conf, db_name, db_table, debug=False):
        #dbconf = {'master':{'host':'192.168.15.63', 'user':'syd', 'passwd':'iou765'}}
        self.debug = debug
        self.db_pool_conf = db_pool_conf
        self.db_name = db_name
        self.db_table = db_table
        #self.lightcloud = LightCloud.connect(lightcloud_node)
        self.connector = DatabaseConnector(dbconf)
        self.pool_db = self.connector.get(db_name, db_table)
    
    def insert(self, page):
        logger.debug('inserting %s'%page.url)
        json_data = ''
        now = datetime.now()
        now_str = strftime("%Y-%m-%d %X", datetime.timetuple(now))

        for rtry in xrange(0, 3):
            try:
                logger.debug('Connecting to database master.supermario')
                conn = self.pool_db.get()
                break
            except ConnectTimeout:
                logger.error(Traceback())
                if rtry >1 : return
                self.connector = DatabaseConnector(self.db_pool_conf)
                self.pool_db = self.connector.get(self.db_name, self.db_table)
        c = conn.cursor(DictCursor)
        
        if not page.author: page.author = u''
        if not page.tags: page.tags = u''
        data = (('url', page.url), ('url_hash', page.url_hash), ('title', page.title), ('content', page.content), ('author', page.author), ('tags', repr(page.tags)), ('published_at', strftime("%Y-%m-%d %X", datetime.timetuple(page.published_at))), ('identifier', page.identifier), ('inserted_at', now_str),)
        try:
            c.execute("SELECT * FROM items WHERE url_hash=%s", (page.url_hash, ))
            r = c.fetchone()
            if not r:
                c.execute("INSERT INTO items (url, url_hash, title, content, author, tags, published_at, identifier, inserted_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (page.url.encode('utf-8').decode('latin1'), page.url_hash.encode('utf-8').decode('latin1'), page.title.encode('utf-8'), page.content.encode('utf-8').decode('latin1'), page.author.encode('utf-8').decode('latin1'), simplejson.dumps(page.tags).encode('utf-8').decode('latin1'), strftime("%Y-%m-%d %X", datetime.timetuple(page.published_at)), page.identifier.encode('utf-8'), now_str, now_str))
                logger.debug('inserted %s, %s'%(page.url_hash, page.url))
            else:
                c.execute("UPDATE items SET url=%s, title=%s, content=%s, author=%s, tags=%s, published_at=%s, updated_at=%s, identifier=%s WHERE url_hash=%s", (page.url.encode('utf-8').decode('latin1'), page.title.encode('utf-8').decode('latin1'), page.content.encode('utf-8').decode('latin1'), page.author.encode('utf-8').decode('latin1'), simplejson.dumps(page.tags).encode('utf-8').decode('latin1'), strftime("%Y-%m-%d %X", datetime.timetuple(page.published_at)), now_str, page.identifier.encode('utf-8'), page.url_hash.encode('utf-8').decode('latin1')))
                logger.debug('updated %s, %s'%(page.url_hash, page.url))
            conn.commit()
        except:
            logger.error(Traceback())
        finally:
            try:
                self.pool_db.put(conn)
            except:
                logger.error(Traceback())
        c.close()
        try:
            json_data = simplejson.dumps(data)
        except:
            json_data = ''
        #self.lightcloud.set(LightCloud.item_key(page.url), json_data)
        return


class PageFeeder:

    def __init__(self, inserter, debug=0):
        self.inserter = inserter
        self.debug = debug
        return

    def feed_page(self, entry):
        if not entry: return
        self.inserter.insert(entry)
        logger.debug('Added: %s, %s' % (entry.url_hash, entry.url))
        return

    def close(self):
        return


def Inserter(db_pool_conf, db_name, db_table, debug=False):
    '''
    input_path: where the parsed data stored
    archive_path: move the inserted data file to which path
    sqlite_path: the path have the sqlite db file
    '''
    archives = []
    inserter = BaseInserter(db_pool_conf, db_name, db_table, debug)
    for entry in Entry.all():
        feeder = PageFeeder(inserter, debug=debug)
        feeder.feed_page(entry)
        feeder.close()
        entry.delete()
    logger.debug('FINISHED')