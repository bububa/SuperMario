#!/usr/bin/env python
# encoding: utf-8
"""
parser.py
SuperMario

Created by Syd on 2009-08-16.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""

import sys
import os
import shutil
import time
import re
from hashlib import md5
import feedparser
import operator
import logging
from datetime import datetime
import feedparser
from htmlentitydefs import name2codepoint
from BeautifulSoup import BeautifulSoup
from bububa.SuperMario.Mario import Mario, MarioRss
from bububa.SuperMario.layout_analyzer import sigchars, get_textblocks, retrieve_blocks
from bububa.SuperMario.utils import URL, Traceback, guess_baseurl, levenshtein, levenshtein_distance, lcs
from bububa.SuperMario.bsp import BSP
try:
    from bububa.SuperMario.MongoDB import *
except:
    pass
    
try: 
    from cStringIO import StringIO 
except ImportError: 
    from StringIO import StringIO

logger = logging.getLogger("Parser")
handler = logging.StreamHandler()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

stderr = sys.stderr

class ParserError(RuntimeError): pass

# This pattern has been carefully tuned, but re.search can still cause a
# stack overflow.  Try re.search('(a|b)*', 'a'*10000), for example.
tagcontent_re = r'''(('[^']*'|"[^"]*"|--([^-]+|-[^-]+)*--|-(?!-)|[^'">-])*)'''

def tag_re(tagname_re):
    return '<' + tagname_re + tagcontent_re + '>'
anytag_re = tag_re(r'(\?|!\w*|/?[a-zA-Z_:][\w:.-]*)')
tagpat = re.compile(anytag_re)

def no_groups(re):
    return re.replace('(', '(?:').replace('(?:?', '(?')

# This pattern matches a character entity reference (a decimal numeric
# references, a hexadecimal numeric reference, or a named reference).
charrefpat = re.compile(r'&(#(\d+|x[\da-fA-F]+)|[\w.:-]+);?')
tagsplitter = re.compile(no_groups(anytag_re))
parasplitter = re.compile(no_groups(tag_re('(p|/p|table|/table|form|/form)')), re.I)
linesplitter = re.compile(no_groups(tag_re('(div|/div|br|tr)')), re.I)
cdatapat = re.compile(r'<(!\s*--|style\b|script\b)', re.I)
endcdatapat = {'!': re.compile(r'--\s*>'),
                'script': re.compile(r'</script[^>]*>', re.I),
                'style': re.compile(r'</style[^>]*>', re.I)}

def replace_valid_tags(text, tags):
    for t in iter(tags):
        p1 = re.compile(r'<%s(\s.*?|)>'%t, re.I)
        p2 = re.compile(r'</%s(\s.*?|)>'%t, re.I)
        if t in ('h%d'%i for i in xrange(1, 7)):
            text = p1.sub('<{#strong#}>', text)
            text = p2.sub('<{#strong#}>', text)
        else:
            text = p1.sub('<{#%s#}>'%t, text)
            text = p2.sub('<{#/%s#}>'%t, text)
    return text

def recover_valid_tags(text, tags):
    for t in iter(tags):
        p1 = re.compile(r'<{#%s#}>'%t, re.I)
        p2 = re.compile(r'<{#/%s#}>'%t, re.I)
        text = p1.sub('<%s>'%t, text)
        text = p2.sub('</%s>'%t, text)
    return text

def replace_image_tags(html):
    pattern = re.compile(r'<image([^^].*?)>', re.I|re.S|re.U)
    imagesWrappers = pattern.findall(html)
    if not imagesWrappers: return
    p1 = re.compile('src="([^^].*?)"', re.I|re.U)
    p2 = re.compile('title="([^^].*?)"', re.I|re.U)
    for w in imagesWrappers:
        src = title = ""
        tmp = p1.findall(w)
        if tmp: src = tmp[0]
        tmp = p2.findall(w)
        if tmp: title = tmp[0]
        if not src: continue
        re.sub('<image%s>'%w, '[image src="%s" title="%s"]'%(src, title), html)
    
def striptags(html):
    """Strip HTML tags from the given string, yielding line breaks for DIV,
    BR, or TR tags and blank lines for P, TABLE, or FORM tags."""
    # Remove comments and elements with CDATA content (<script> and <style>).
    # These are special cases because tags are not parsed in their content.
    replace_image_tags(html)
    chunks, pos = [], 0
    while 1:
        startmatch = cdatapat.search(html, pos)
        if not startmatch:
            break
        tagname = startmatch.group(1).rstrip('-').strip()
        tagname = tagname.lower()
        endmatch = endcdatapat[tagname].search(html, startmatch.end())
        if not endmatch:
            break
        chunks.append(html[pos:startmatch.start()])
        pos = endmatch.end()
    chunks.append(html[pos:])
    html = ''.join(chunks)
    
    # Break up the text into paragraphs and lines, then remove all other tags.
    paragraphs = []
    for paragraph in iter(parasplitter.split(html)):
        lines = []
        for line in iter(linesplitter.split(paragraph)):
            line = replace_valid_tags(line, ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b', 'u'])
            line = ''.join(tagsplitter.split(line))
            line = recover_valid_tags(line, ['strong', 'b', 'u'])
            #nbsp = (type(line) is unicode) and u'\xa0' or '\xa0'
            #line = line.replace(nbsp, ' ')
            lines.append(' '.join(line.split()))
        paragraph = '\n'.join(lines)
        paragraphs.append(paragraph)
    return re.sub('\n\n+', '\n\n', '<p>'.join(paragraphs)).strip()


class LayoutSection:
    def __init__(self, id, diffscore, mainscore, blocks):
        self.id = id
        self.diffscore = diffscore
        self.mainscore = mainscore
        self.weight = sum( b.weight for b in blocks )
        self.blocks = blocks
        return


class LayoutPattern:
    def __init__(self, name, score, title_sectno, main_sectno, pattern):
        self.name = name
        self.score = score
        self.title_sectno = title_sectno
        self.main_sectno = main_sectno
        self.pattern = pattern
        return
    
    def match_blocks(self, blocks0, strict=True):
        diffs = [ d for (d,m,p) in self.pattern ]
        mains = [ m for (d,m,p) in self.pattern ]
        paths = [ p for (d,m,p) in self.pattern ]
        layout = []
        for (diffscore,mainscore,blocks1) in zip(diffs, mains, retrieve_blocks(paths, blocks0)):
            if strict and not blocks1:
                return None
            layout.append(LayoutSection(len(layout), diffscore, mainscore, blocks1))
        return layout
    
class LayoutPatternSet:
    def __init__(self, debug=0):
        self.pats = []
        self.debug = debug
        return
    
    def read(self, pac):
        for line in pac.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'): continue
            x = eval(line)
            if len(x) == 5:
                (score, name, title_sectno, main_sectno, pattern) = x
            else:
                (score, name, title_sectno, pattern) = x
                main_sectno = -1
            self.pats.append(LayoutPattern(name, score, title_sectno, main_sectno, pattern))
        return
    
    def identify_layout(self, tree, pat_threshold=0.9, strict=True):
        top = (None, None)
        blocks = get_textblocks(tree)
        if 2 <= self.debug:
            tree.dump()
        max_weight = sum( b.weight for b in blocks ) * pat_threshold
        for pat1 in self.pats:
            layout = pat1.match_blocks(blocks, strict=strict)
            if layout:
                weight = sum( sect.weight for sect in layout )
                if max_weight < weight:
                    top = (pat1, layout)
                    max_weight = weight
        return top
    
    def dump_text(self, name, tree, pat_threshold=0.9, diffscore_threshold=0.5, main_threshold=10, codec_out='utf-8', strict=True):
        enc = lambda x: x.encode(codec_out, 'replace')
        (pat1, layout) = self.identify_layout(tree, pat_threshold, strict=strict)
        if not layout:
            logger.debug('!UNMATCHED: %s' % name)
            return
        logger.debug('!MATCHED: %s' % name)
        logger.debug('PATTERN: %s' % pat1.name)
        if self.debug:
            for sect in layout:
                logger.debug('DEBUG: SECT-%d: diffscore=%.2f' % (sect.id, sect.diffscore))
                for b in sect.blocks:
                    logger.debug('   %s' % enc(b.orig_text))
        for sectno in xrange(len(layout)):
            sect = layout[sectno]
            if sectno == pat1.title_sectno:
                for b in sect.blocks:
                    logger.debug('TITLE: %s' % enc(b.orig_text))
            elif diffscore_threshold <= sect.diffscore:
                if pat1.title_sectno < sectno and main_threshold <= sect.mainscore:
                    for b in sect.blocks:
                        logger.debug('MAIN-%d: %s' % (sect.id, enc(b.orig_text)))
                else:
                    for b in sect.blocks:
                        logger.debug('SUB-%d: %s' % (sect.id, enc(b.orig_text)))
        return
    
    
class HTMLPage:
    
    def __init__(self, name, url, effective_url, tree, html):
        self.name = name
        self.url = url
        self.effective_url = effective_url
        self.tree = tree
        self.html = html
        return
    
    def __repr__(self):
        return '<%s>' % self.name

class LayoutParser:
    
    def __init__(self, identifier, pac, debug=False):
        self.pacs = {}
        self.debug = debug
        self.pages = {}
        self.identifier = identifier
        self.rss = None
        self.pac = pac
    
    def add_rss(self, rss):
        self.rss = rss
      
    def get_full_pac(self, identifier, tree):
        if self.pac and isinstance(self.pac, unicode):
            pac = self.pac
        else:
            site = Site.one({'url_hash':identifier})
            if not site or not site.pattern:
                logger.debug('No pattern for %s'%identifier)
                return None
            pac = site.pattern
        return pac
    
    def parse_pac(self, pattern, tree):
        patternset = LayoutPatternSet(debug=self.debug)
        try:
            patternset.read(pattern)
        except:
            logger.debug('Full pac file error: %r'%Traceback())
            return None
        if self.rss: pat_threshold = 0.0
        else: pat_threshold = 0.9
        (item_pattern, layout) = patternset.identify_layout(tree, pat_threshold=pat_threshold)
        if not item_pattern: 
            logger.debug("Fail to get matched pattern")
            return None
        pac = {'url':'','title':'','content':''}
        if item_pattern.title_sectno > 0: pac['title'] = item_pattern.pattern[item_pattern.title_sectno]
        tmp = sorted(item_pattern.pattern, key=operator.itemgetter(1))
        for t in tmp[::-1]:
            if t[2]!='title':
                pac['content'] = t
                break
        logger.debug(repr(pac))
        tmp = []
        del tmp
        return pac
        
    def accept_url(self, url, tree):
        if self.pac and isinstance(self.pac, dict): 
            pac = self.pac
            identifier = self.identifier
        else:
            baseurl, rssurl, rssbody = guess_baseurl(url, tree)
            identifier = md5(baseurl).hexdigest()
            if identifier not in self.pacs:
                pac = self.get_full_pac(identifier, tree)
                self.pacs[identifier] = pac
            else:
                pac = self.pacs[identifier]
        if not pac:
            AddToAnalyzerCandidates(identifier, url)
            return None
        if self.rss: return True
        try:
            return re.match(pac['url'], url)
        except:
            logger.debug('without rss and pac url')
            #AddToAnalyzerCandidates(identifier, url)
            return None
        
    def add_tree(self, name, tree, html, url, effective_url):
        if not self.accept_url(effective_url, tree):
            logger.debug('reject: %s, %s' % (name, effective_url))
            return 0
        page = HTMLPage(name, url, effective_url, tree, html)
        self.pages[name] = page
        return len(self.pages)
    
    def get_elements(self, string):
        return tuple((x[0], dict(tuple(a.split('=')) for a in x[1:])) for x in (e.split(':') for e in string.split('/') if e))
    
    def find_element(self, tree, elements, pre_elms, debug=False):
        if not elements: return None
        if len(elements[0]) > 1:
            elms = tree.findAll(elements[0][0], attrs=elements[0][1])
        else:
            elms = tree.findAll(elements[0][0])
        if not elms: 
            return None
        if len(elements) == 1:
            if len(elms) == 1: return elms[0]
            elif pre_elms:
                for e in pre_elms:
                    if e == tree: return pre_elms[0]
                return None
            else: return None
        for element in elms:
            res = self.find_element(element, elements[1:], elms, debug)
            if res: return res
        
    def extract(self, page, field, pac):
        logger.debug('Extract: %s, %s'%(page.name, field))
        if not pac: return None
        if field == 'tags' and pac.has_key('tags'):
            p = re.compile(pac['tags'], re.I)
            res = p.findall(page.html)
            if res:
                p = re.compile(r'<a[^^].*?>([^^].?)</a>', re.I)
                tags = p.findall(res[0])
                if tags: return tags
        if pac.has_key('type') and pac['type'] == 'regx':
            pattern = re.compile(pac[field], re.I)
            res = pattern.findall(page.html)
            if res: return [t.strip() for t in res[0].contents[0] if t.strip()]
        else:
            if field not in pac or not pac[field]: return None
            elements = self.get_elements(pac[field][2])
            debug=False
            element = self.find_element(page.tree, elements, page.tree, debug)
            if element: 
                #return element.renderContents().strip()
                return striptags(element.renderContents())
        return None
    
    def extract_entry_from_rss(self, url):
        for entry in self.rss['entries']:
            if url == entry['link']:
                print 'found: %s'%url
                return entry
        return None
        
    def parse(self, page):
        if self.pac and isinstance(self.pac, dict): 
            pac = self.pac
            identifier = self.identifier
        else: 
            baseurl, rssurl, rssbody = guess_baseurl(page.effective_url, page.tree)
            identifier = md5(baseurl).hexdigest()
            if self.pacs.has_key(identifier): pac = self.pacs[identifier]
            else: pac = None
            if isinstance(pac, unicode): pac = self.parse_pac(pac, page.tree)
        if not pac:
            AddToAnalyzerCandidates(identifier, page.effective_url)
            return
        logger.debug('parse: %s, %s' % (page.name, page.effective_url))
        data = {}
        rss_entry = None
        if self.rss: rss_entry = self.extract_entry_from_rss(page.effective_url)
        for k in pac:
            if k not in ('url', 'effective_url', 'type'):
                if rss_entry and k=='title':
                    data[k] = rss_entry.title
                    continue
                data[k] = self.extract(page, k, pac)
        if 'title' not in data or not data['title']:
            try:
                data['title'] = page.tree.title.string
            except:
                pass
        if not (data['title'] and data['content']):
            AddToAnalyzerCandidates(identifier, page.effective_url)
        if (not data['content'] or len(data['content']) < 50) and rss_entry:
            data['content'] = rss_entry.description
        if not data['title'] or not data['content']:
            logger.debug('Ignore item because of no title or content')
            return
        data['url'] = page.effective_url
        data['identifier'] = identifier
        if rss_entry:
            try:
                data['published_at'] = datetime(*rss_entry.updated_parsed[:6])
            except:
                pass
            try:
                data['author'] = rss_entry.author
            except:
                pass
        url_hash = md5(data['url']).hexdigest()
        entry = Entry.one({'url_hash': url_hash})
        if not entry:
            entry = Entry()
            entry.url = data['url'] if isinstance(data['url'], unicode) else data['url'].decode('utf-8')
            entry.url_hash = url_hash if isinstance(url_hash, unicode) else url_hash.decode('utf-8')
            entry.identifier = identifier if isinstance(identifier, unicode) else identifier.decode('utf-8')
        entry.title = data['title'] if isinstance(data['title'], unicode) else data['title'].decode('utf-8')
        entry.content = data['content'] if isinstance(data['content'], unicode) else data['content'].decode('utf-8')
        if data.has_key('author') and data['author']:
            entry.author = data['author'] if isinstance(data['author'], unicode) else data['author'].decode('utf-8')
        if data.has_key('published_at') and data['published_at']:
            entry.published_at = data['published_at']
        entry.updated_at = datetime.utcnow()
        entry.save()
        
    
    def run(self):
        for name in iter(self.pages):
            self.parse(self.pages[name])


class PageFeeder:
    
    def __init__(self, parser, debug=False):
        self.parser = parser
        self.debug = debug
    
    def feed_rss(self, rss):
        try:
            self.parser.add_rss(feedparser.parse(rss['body'].encode('utf-8')))
        except:
            logger.error("Can't parse rss. %s"%Traceback())

    def feed_page(self, version):
        page = Page.one({'_id': version.page})
        url = version.url
        try:
            name = md5(url).hexdigest()
        except:
            name = md5(url.encode('utf-8')).hexdigest()
        effective_url = page.url
        data = version.raw
        try:
            tree = BeautifulSoup(data, fromEncoding='utf-8')
        except:
            return
        n = self.parser.add_tree(name, tree, data, url, effective_url)
        logger.debug('Added: %d: %s, %s' % (n, name, url))
        return
    
    def close(self):
        self.parser.run()

def AddToAnalyzerCandidates(identifier, starturl):
    ac = AnalyzerCandidate.one({'url_hash':identifier})
    if not ac:
        ac = AnalyzerCandidate()
        ac.url_hash = identifier if isinstance(identifier, unicode) else identifier.decode('utf-8')
        ac.start_url = starturl if isinstance(starturl, unicode) else starturl.decode('utf-8')
    ac.inserted_at = datetime.utcnow()
    ac.save()
    return


def Parser(identifier, pac=None, debug=False):
    site_info = Site.one({'url_hash':identifier})
    if not site_info: return
    '''bsp = BSP()
    bsp_pac = bsp.get_pac(site_info.url)
    if bsp_pac:
        pac = bsp_pac'''
    if not pac:
        pac = site_info.pattern
    if not pac:
        logger.error("Can't find pac for site %s"%identifier)
        AddToAnalyzerCandidates(identifier, site_info.url)
        for box in PageSandbox.all({'analyzer': 0, 'mixed': 0}):
            box.delete()
            logger.debug('FINISHED BOX: %s'%box._id)
        logger.debug('FINISHED')
        return
    parser = LayoutParser(identifier, pac, debug=debug)
    for box in PageSandbox.all({'analyzer': 0, 'mixed': 0}):
        feeder = PageFeeder(parser=parser, debug=debug)
        if box.rss: feeder.feed_rss(box.rss)
        for version in box.page_versions:
            version = PageVersion.one({'_id':version})
            if not version: continue
            feeder.feed_page(version)
        feeder.close()
        box.delete()
        logger.debug('FINISHED BOX: %s'%box._id)
    logger.debug('FINISHED')


def MixedRssParser(identifier, debug=False):
    parser = LayoutParser(identifier, None, debug=debug)
    for box in PageSandbox.all({'analyzer': 0, 'mixed': 1}):
        feeder = PageFeeder(parser=parser, debug=debug)
        if box.rss: feeder.feed_rss(box.rss)
        for version in box.page_versions:
            version = PageVersion.one({'_id':version})
            if not version: continue
            feeder.feed_page(version)
        feeder.close()
        box.delete()
        logger.debug('FINISHED BOX: %s'%box._id)
    logger.debug('FINISHED')


class FullRssParser:
    
    def __init__(self, url, etag=None, last_modified=None, proxy=None, callback=None, check_baseurl=2, multithread=False, debug=False):
        self.baseurl = None
        self.mario = MarioRss(callback=self.rss_parser)
        self.callback = callback
        self.check_baseurl = check_baseurl
        self.multithread = multithread
        self.rss_response = self.mario.get(starturl=None, rssurl=url, etag=etag, last_modified=last_modified, proxy=proxy, multithread=self.multithread)
        self.debug = debug
    
    def rss_parser(self, response):
        if not response or not response.body: return None
        try:
            tree = BeautifulSoup(response.body, fromEncoding='utf-8')
        except Exception, err:
            return None
        feed = self.matched_feed(response)
        if not feed: return None
        blocks = get_textblocks(tree.body)
        if not blocks: return None
        res_blocks = []
        try:
            feed_content = striptags(feed['summary'])
        except KeyError:
            feed_content = striptags(feed['content'][0]['value'])
        for b in blocks:
            dist = levenshtein_distance(feed_content, striptags(b.orig_text))
            #dist = len(lcs(striptags(feed['summary']), b.orig_text))
            res_blocks.append((dist, b))
        res_blocks = sorted(res_blocks, key=operator.itemgetter(0))
        block = res_blocks[0][1]
        blocks = None
        lp = LayoutParser(None, None)
        elements = lp.get_elements(block.path)
        element = lp.find_element(tree.body, elements, tree.body, False)
        if element: 
            #return element.renderContents().strip()
            content = striptags(element.renderContents())
            if len(content) < len(feed_content):
                content = feed_content
        else:
            content = feed_content
        url = response.effective_url.decode('utf-8')
        if isinstance(content, str): content = content.decode('utf-8')
        try:
            author = feed.author.decode('utf-8')
        except:
            author = u''
        if self.callback:
            if self.check_baseurl == 1 and not self.baseurl or self.check_baseurl !=None:
                self.baseurl = guess_baseurl(url, tree)
            else:
                self.baseurl = None
            if feed.has_key('updated_parsed'): updated_parsed = feed['updated_parsed']
            else: updated_parsed = None
            self.callback({'url':url, 'title':striptags(feed['title']), 'content':content, 'updated_parsed':updated_parsed, 'author':author, 'baseurl':self.baseurl, 'etag':response.etag, 'last_modified':response.last_modified})
        
    def matched_feed(self, response):
        links = self.mario.link_title_db.dic
        for k, v in links.iteritems():
            link = URL.normalize(k)
            if link in (response.url, response.effective_url):
                return v[0][2]
        return None