#!/usr/bin/env python
# encoding: utf-8
"""
Keywords.py

Created by Syd on 2009-08-29.
Copyright (c) 2009 __MyCompanyName__. All rights reserved.
"""

import sys
import os
import re
from urllib import quote
import simplejson
from BeautifulSoup import BeautifulSoup
from bububa.SuperMario.Mario import Mario

stderr = sys.stderr

class GoogleSuggestKeywords:

    def __init__(self, debug=0):
        self.debug = debug
        self.url_struct = 'http://www.google.com/search?client=safari&rls=en&q=%s&ie=UTF-8&oe=UTF-8'

    def get(self, keyword):
        keyword = quote(keyword)
        url = self.url_struct%keyword
        mario = Mario(None)
        response = mario.get(url)
        if response:
            return self.parse(response.body)
        return []

    def parse(self, html):
        pattern = re.compile(r'<div class=e[^^]*?<caption[^^]*?Searches related to: [^^]*?<td[^^]*?>([^^]*?)</div>', re.I)
        wrapper = pattern.findall(html)
        if not wrapper: 
            if self.debug:
                print >>stderr, "can't find!"
            return []
        soup = BeautifulSoup(wrapper[0], fromEncoding='utf-8')
        return [''.join(a.findAll(text=True)) for a in soup.findAll('a')]

class GoogleCNSuggestKeywords:
    
    def __init__(self, debug=0):
        self.debug = debug
        self.url_struct = 'http://www.google.cn/search?hl=zh-CN&newwindow=1&q=%s&aq=f&oq='

    def get(self, keyword):
        keyword = quote(keyword)
        url = self.url_struct%keyword
        mario = Mario(None)
        response = mario.get(url)
        if response:
            return self.parse(response.body)
        return []

    def parse(self, html):
        pattern = re.compile(r'<div class=e><table class=ts>[^^]*?相关搜索[^^]*?<td[^^]*?>([^^]*?)</div>', re.I)
        wrapper = pattern.findall(html)
        if not wrapper: 
            if self.debug:
                print >>stderr, "can't find!"
            return []
        soup = BeautifulSoup(wrapper[0], fromEncoding='utf-8')
        return [''.join(a.findAll(text=True)) for a in soup.findAll('a')]
        

class GoogleJpSuggestKeywords:

    def __init__(self, debug=0):
        self.debug = debug
        self.url_struct = 'http://www.google.co.jp/search?hl=ja&q=%s&btnG=検索&lr=lang_ja'

    def get(self, keyword):
        keyword = quote(keyword)
        url = self.url_struct%keyword
        mario = Mario(None)
        response = mario.get(url)
        if response:
            return self.parse(response.body)
        return []

    def parse(self, html):
        pattern = re.compile(r'<div class=e[^^]*?<caption[^^]*?他のキーワード: [^^]*?<td[^^]*?>([^^]*?)</div>', re.I)
        wrapper = pattern.findall(html)
        if not wrapper: 
            if self.debug:
                print >>stderr, "can't find!"
            return []
        soup = BeautifulSoup(wrapper[0], fromEncoding='utf-8')
        return [''.join(a.findAll(text=True)) for a in soup.findAll('a')]

class BaiduSuggestKeywords:

    def __init__(self, debug=0):
        self.debug = debug
        self.url_struct = 'http://www.baidu.com/s?f=8&wd=%s'

    def get(self, keyword):
        keyword = keyword.decode('utf-8').encode('gbk')
        keyword = quote(keyword)
        url = self.url_struct%keyword
        mario = Mario(None)
        response = mario.get(url)
        if response:
            return self.parse(response.body)
        return []

    def parse(self, html):
        pattern = re.compile(r'<div[^^]*?><table[^^]*?>相关搜索<[^^]*?<td[^^]*?>([^^]*?)</div>', re.I)
        wrapper = pattern.findall(html)
        if not wrapper: 
            if self.debug:
                print >>stderr, "can't find!"
            return []
        soup = BeautifulSoup(wrapper[0], fromEncoding='utf-8')
        return [''.join(a.findAll(text=True)) for a in soup.findAll('a')]


class YahooSuggestKeywords:
    
    def __init__(self, debug=0):
        self.debug = debug
        self.url_struct = 'http://sugg.search.yahoo.com/gossip-us-sayt/?output=yjsonp&nresults=10&l=1&command=%s'
    
    def get(self, keyword):
        keyword = quote(keyword)
        url = self.url_struct%keyword
        mario = Mario(None)
        response = mario.get(url)
        if response:
            return self.parse(response.body)
        return []
    
    def parse(self, html):
        pattern = re.compile(r'yasearch\(([^^]*?)\)', re.I)
        wrapper = pattern.findall(html)
        if not wrapper: 
            if self.debug:
                print >>stderr, "can't find!"
            return []
        res = simplejson.loads(wrapper[0])
        try:
            return [t[0] for t in res['r']]
        except:
            return []

class BingSuggestKeywords:

    def __init__(self, lang='en-US', debug=0):
        self.debug = debug
        self.lang = lang
        self.url_struct = 'http://www.bing.com/search?q=%s&go=&form=QBRE&mkt=%s&setlang=SET_NULL'

    def get(self, keyword):
        keyword = quote(keyword)
        url = self.url_struct%(keyword, self.lang)
        mario = Mario(None)
        response = mario.get(url)
        if response:
            return self.parse(response.body)
        return []

    def parse(self, html):
        if self.lang == 'en-US': h2 = 'Related searches'
        elif self.lang == 'zh-CN': h2='相关搜索'
        else: return []
        pattern = re.compile(r'<h2>%s</h2><ul>([^^]*?)</ul>'%h2, re.I)
        wrapper = pattern.findall(html)
        if not wrapper: 
            if self.debug:
                print >>stderr, "can't find!"
            return []
        soup = BeautifulSoup(wrapper[0], fromEncoding='utf-8')
        return [''.join(a.findAll(text=True)) for a in soup.findAll('a')]

    
class SuggestKeywords:
    
    def __init__(self, engines=[GoogleSuggestKeywords(), YahooSuggestKeywords(), BingSuggestKeywords()], debug=0):
        self.debug = debug
        self.engines = engines
    
    def get(self, keyword):
        if not self.engines: return None
        keywords = []
        for engine in self.engines:
            keywords.extend(engine.get(keyword))
        return set(keywords)
