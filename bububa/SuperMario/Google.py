#!/usr/bin/env python
# encoding: utf-8
"""
Google.py

Created by Syd on 2010-01-25.
Copyright (c) 2010 __ThePeppersStudio__. All rights reserved.
"""

import sys
import os
import re
from dateutil.parser import parse as dateParse
from googleanalytics import Connection
from googleanalytics.exception import GoogleAnalyticsClientError
from bububa.SuperMario.Mario import Mario

class GoogleResult:
    
    def __init__(self, url, unescape_url, title, description, ranking):
        self.url = url
        self.unescape_url = unescape_url
        self.title = title
        self.description = description
        self.ranking = ranking
    
    def __repr__(self):
        return '<%s(url=%s,title=%s, ranking=%d)>'%(self.__class__.__name__, self.url, self.title, self.ranking)
    

class GoogleSearch:
    SEARCH_URL_WITH_NUMBER = "%(domain)s/search?hl=en&q=%(query)s&num=%(num)d&btnG=Google+Search"
    SEARCH_URL = "%(domain)s/search?hl=en&q=%(query)s&btnG=Google+Search"
    NEXT_PAGE = "%(domain)s/search?hl=en&q=%(query)s&start=%(start)d"
    NEXT_PAGE_WITH_NUMBER = "%(domain)s/search?hl=en&q=%(query)s&num=%(num)d&start=%(start)d"
    
    def __init__(self, query=None, number_of_results=100, domain='http://www.google.com'):
        self.query = query
        self.domain = domain
        self.number_of_results = number_of_results
        self.proxies = None
    
    def set_proxies(self, proxies):
        self.proxies = proxies
    
    def count(self, query=None, domain=None):
        if not domain: domain = self.domain
        if not query: query = self.query
        url = GoogleSearch.SEARCH_URL%{'domain':domain, 'query':query}
        mario = Mario()
        mario.set_proxies_list(self.proxies)
        response = mario.get(url)
        if not response:
            raise GoogleException('Fail to open page', 502)
        patterns = [re.compile('<p id=resultStats>&nbsp;[^^]*?<b>\d+</b> - <b>\d+</b>[^^]*?<b>([^^]*?)</b>'), re.compile('<p id=resultStats>&nbsp;[^^]*?<b>[^^]*?</b>[^^]*?<b>([^^]*?)</b>[^^]*?<b>\d+</b>-<b>\d+</b>')]
        for pattern in patterns:
            res = pattern.findall(response.body)
            if not res: continue
            return long(re.sub(',', '', res[0]))
        return 0
    
    def search(self, query=None, number_of_pages=1, domain=None):
        if not domain: domain = self.domain
        if not query: query = self.query
        results = []
        for page in xrange(0, number_of_pages):
            results.extend(self._get_page(query, page, domain))
        return results
    
    def _get_page(self, query, page, domain):
        if page == 0:
            if self.number_of_results == 10:
                url = GoogleSearch.SEARCH_URL%{'domain':domain, 'query':query}
            else:
                url = GoogleSearch.SEARCH_URL_WITH_NUMBER%{'domain':domain, 'query':query, 'num':self.number_of_results}
        else:
            if self.number_of_results == 10:
                url = GoogleSearch.NEXT_PAGE%{'domain':domain, 'query':query, 'start':page*self.number_of_results}
            else:
                url = GoogleSearch.NEXT_PAGE_WITH_NUMBER%{'domain':domain, 'query':query, 'num':self.number_of_results, 'start':page*self.number_of_results}
        mario = Mario()
        mario.set_proxies_list(self.proxies)
        response = mario.get(url)
        if not response:
            raise GoogleException('Fail to open page', 502)
        results = self._parse_response(response.body)
        if not results: return []
        return [GoogleResult('http://%s'%result['url'], result['unescape_url'], result['title'], result['description'], page*self.number_of_results+i+1) for i, result in enumerate(results)]
    
    def _parse_response(self, page):
        pattern = re.compile('<!--m--><li([^^]*?)<!--n-->', re.S)
        wrappers = pattern.findall(page)
        if not wrappers: return None
        patterns = {'unescape_url':'<h3 class=r><a href="([^^]*?)"[^^]*?</h3>', 'title':'<h3 class=r><a[^^]*?>([^^]*?)</a></h3>', 'description':'<div class="s">([^^]*?)。<br>', 'url':'<cite>([^^]*?) - </cite>'}
        response = []
        for wrapper in wrappers:
            tmp = {}
            for key, pattern in patterns.items():
                pattern = re.compile(pattern, re.S)
                res = pattern.findall(wrapper)
                if res: tmp[key] = res[0]
                else: tmp[key] = None
            response.append(tmp)
        return response
    

class GoogleAnalytics:
    MAX_RESULTS = 50000
    def __init__(self, email, password, account_id=None):
        self.email = email
        self.password = password
        try:
            self.connection = Connection(self.email, self.password)
        except GoogleAnalyticsClientError, e:
            raise GoogleExecption(e, 504)
        self.set_account(account_id)
    
    def set_account(self, account_id):
        self.account = None
        if account_id:
            if isinstance(account_id, (int, long)): account_id = str(account_id)
            self.account = self.connection.get_account(account_id)
    
    def accounts(self):
        return self.connection.get_accounts()
    
    def page_traffics(self, filters, start_date, end_date):
        if not self.account: raise GoogleException('Please set account id.', 504)
        if isinstance(start_date, (str, unicode)): start_date = dateParse(start_date)
        if isinstance(end_date, (str, unicode)): end_date = dateParse(end_date)
        if isinstance(filters, (str, unicode)): filters = eval(filters)
        data = self.account.get_data(start_date, end_date, metrics=['visits',], dimensions=['pagePath', ], filters=filters, sort=['-visits',], max_results=GoogleAnalytics.MAX_RESULTS)
        return data
    
    def keyword_traffics(self, filters, start_date, end_date):
        if not self.account: raise GoogleException('Please set account id.', 504)
        if isinstance(start_date, (str, unicode)): start_date = dateParse(start_date)
        if isinstance(end_date, (str, unicode)): end_date = dateParse(end_date)
        if isinstance(filters, (str, unicode)): filters = eval(filters)
        data = self.account.get_data(start_date, end_date, metrics=['visits',], dimensions=['keyword', ], filters=filters, sort=['-visits',], max_results=GoogleAnalytics.MAX_RESULTS)
        return data
    

class GoogleException(Exception):

    def __init__(self, msg, code=None):
        self.msg = msg
        self.code = code

    def __str__(self):
        return repr(self.msg)
