#!/usr/bin/env python
# encoding: utf-8
"""
Storage.py

Created by Syd on 2009-08-06.
Copyright (c) 2009 __MyCompanyName__. All rights reserved.
"""

import sys
import os
import time
from hashlib import md5
import MySQLdb
from eventlet import db_pool

class Storage:
    
    def __init__(self):
        '''
            General storage class
        '''


class ConnectionPool(db_pool.TpooledConnectionPool):
    """A pool which gives out saranwrapped MySQLdb connections from a pool
    """
    def __init__(self, *args, **kwargs):
        super(ConnectionPool, self).__init__(MySQLdb, *args, **kwargs)
    
    def get(self):
        conn = super(ConnectionPool, self).get()
        # annotate the connection object with the details on the
        # connection; this is used elsewhere to check that you haven't
        # suddenly changed databases in midstream while making a
        # series of queries on a connection.
        arg_names = ['host','user','passwd','db','port','unix_socket','conv','connect_timeout',
         'compress', 'named_pipe', 'init_command', 'read_default_file', 'read_default_group',
         'cursorclass', 'use_unicode', 'charset', 'sql_mode', 'client_flag', 'ssl',
         'local_infile']
        # you could have constructed this connectionpool with a mix of
        # keyword and non-keyword arguments, but we want to annotate
        # the connection object with a dict so it's easy to check
        # against so here we are converting the list of non-keyword
        # arguments (in self._args) into a dict of keyword arguments,
        # and merging that with the actual keyword arguments
        # (self._kwargs).  The arg_names variable lists the
        # constructor arguments for MySQLdb Connection objects.
        converted_kwargs = dict([ (arg_names[i], arg) for i, arg in enumerate(self._args) ])
        converted_kwargs.update(self._kwargs)
        conn.connection_parameters = converted_kwargs
        return conn
    
    @staticmethod
    def pre_query(conn, dict_cursor=False):
        if not conn: return None
        if dict_cursor:
            cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        else:
            cursor = conn.cursor()
        cursor.execute("SET NAMES utf8")
        cursor.execute("SET CHARACTER SET utf8")
        cursor.execute("SET COLLATION_CONNECTION='utf8_general_ci'")
        return cursor
    
    @staticmethod
    def result_iter(cursor, arraysize=5000):
        while True:
            results = cursor.fetchmany(arraysize)
            if not results: break
            for result in results:
                yield result


class Tables:
    '''
    Create essential database tables
    ''' 
    @staticmethod
    def create_sites(pool_db):
        conn = pool_db.get()
        try:
            cursor = conn.cursor()
            cursor.execute("create table `sites` ( \
                        `id` int unsigned not null auto_increment, \
                        `url` varchar(255) not null, \
                        `url_hash` char(32) not null, \
                        `rss_url` varchar(255) not null default '', \
                        `icon` varchar(255) not null default '', \
                        `avatar` varchar(255) not null default '', \
                        `review` int unsigned not null default '0', \
                        `status` int not null default '0', \
                        `sp` varchar(32) not null default '', \
                        `sp_hash` char(32) not null default '', \
                        `inserted_at` datetime not null, \
                        `updated_at` datetime not null, \
                        `freq` int unsigned not null, \
                        primary key ( `id`),  unique `url_hash`(url_hash), \
                        index `review`(review), index `status`(status), index `sp_hash`(sp_hash), index `inserted_at`(inserted_at), index `updated_at`(updated_at), index `freq`(freq) \
                        )  engine=`InnoDB` default character set utf8 collate utf8_general_ci")
            conn.commit()
            cursor.close()
        finally:
            pool_db.put(conn)
        return
    
    @staticmethod
    def create_site_links(pool_db):
        conn = pool_db.get()
        try:
            cursor = conn.cursor()
            cursor.execute("create table `site_links` ( \
                        `id` int unsigned not null auto_increment, \
                        `url` varchar(255) not null default '', \
                        `url_hash` char(32) not null default '', \
                        `link` varchar(255) not null default '', \
                        `link_hash` varchar(32) not null default '', \
                        `inserted_at` datetime not null, \
                        primary key ( `id`), \
                        index `url`(url), index `url_hash`(url_hash), index `link`(link_hash), index `inserted_at`(inserted_at) \
                        )  engine=`InnoDB` default character set utf8 collate utf8_general_ci")
            conn.commit()
            cursor.close()
        finally:
            pool_db.put(conn)
        return
    
    @staticmethod
    def create_crawled_pages(pool_db):
        conn = pool_db.get()
        try:
            cursor = conn.cursor()
            cursor.execute("create table `crawled_pages` (   \
                            `id` int unsigned not null auto_increment, \
                            `url` varchar(255) not null, \
                            `url_hash` char(32) not null, \
                            `content_hash` char(32) not null default '', \
                            `review` int unsigned not null default '0', \
                            `inserted_at` datetime not null, \
                            `updated_at` datetime not null, \
                            `next_crawl_at` datetime not null, \
                            `review` int unsigned not null default '0', \
                            `identifier` char(32) not null default '', \
                            primary key ( `id`),  unique `url_hash`(url_hash), \
                            index `content_hash`(content_hash), index `review`(review), index `inserted_at`(inserted_at), index `updated_at`(updated_at), index `next_crawl_at`(next_crawl_at), index `identifier`(identifier) \
                            )  engine=`InnoDB` default character set utf8 collate utf8_general_ci")
            conn.commit()
            cursor.close()
        finally:
            pool_db.put(conn)
        return
    
    @staticmethod
    def create_items_db(pool_db):
        conn = pool_db.get()
        try:
            cursor = conn.cursor()
            cursor.execute("create table `items` (   \
                            `id` int unsigned not null auto_increment, \
                            `url` varchar(255) not null, \
                            `url_hash` char(32) not null, \
                            `title` varchar(255) not null default '', \
                            `content` text not null, \
                            `author` varchar(128) not null, \
                            `tags` text not null, \
                            `published_at` datetime not null, \
                            `inserted_at` datetime not null, \
                            `updated_at` datetime not null, \
                            `identifier` char(32) not null default '', \
                            primary key ( `id`),  unique `url_hash`(url_hash), \
                            index `identifier`(identifier), index `inserted_at`(inserted_at), index `published_at`(published_at), index `updated_at`(updated_at) \
                            )  engine=`InnoDB` default character set utf8 collate utf8_general_ci")
            conn.commit()
            cursor.close()
        finally:
            pool_db.put(conn)
        return
            

class DatabaseConnector(db_pool.DatabaseConnector):
    def __init__(self, credentials, *args, **kwargs):
        super(DatabaseConnector, self).__init__(MySQLdb, credentials, conn_pool=db_pool.ConnectionPool, *args, **kwargs)
    
    def get(self, host, dbname, port=3306):
        key = (host, dbname, port)
        if key not in self._databases:
            new_kwargs = self._kwargs.copy()
            new_kwargs['db'] = dbname
            new_kwargs['host'] = host
            new_kwargs['port'] = port
            new_kwargs.update(self.credentials_for(host))
            dbpool = ConnectionPool(*self._args, **new_kwargs)
            self._databases[key] = dbpool
        return self._databases[key]