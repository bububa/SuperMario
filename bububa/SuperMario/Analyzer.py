#!/usr/bin/env python
# encoding: utf-8
"""
analyzer.py
SuperMario

Created by Syd on 2009-08-12.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""

import sys, os, re, random
from math import log
from os.path import dirname
import operator
import logging
try:
    import cPickle as pickle
except:
    import pickle
from hashlib import md5
from BeautifulSoup import BeautifulSoup
from difflib import SequenceMatcher
from bububa.SuperMario.layout_analyzer import sigchars, get_textblocks, retrieve_blocks
from bububa.SuperMario.template import BaseTemplate
from bububa.SuperMario.utils import Traceback
from bububa.SuperMario.MongoDB import Page, PageVersion, Site, PageSandbox

logger = logging.getLogger("Analyzer")
handler = logging.StreamHandler()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

upperbound = min
lowerbound = max
stderr = sys.stderr

def seqmatch(s1, s2):
    if len(s1) < len(s2):
        return [ (b,a,n) for (a,b,n) in SequenceMatcher(None, s2, s1).get_matching_blocks() ]
    else:
        return SequenceMatcher(None, s1, s2).get_matching_blocks()

def titlability(title, body):
    return sum( n*n for (a,b,n) in seqmatch(title, body) ) / float(len(title))

def dice_coeff(s1, s2):
    return 2*sum( n for (a,b,n) in seqmatch(s1, s2) ) / float(len(s1)+len(s2))

def diff_score(s1, s2):
    return len(s1) + len(s2) - 2*sum( n for (a,b,n) in seqmatch(s1, s2) )

def find_lcs(s1, s2):
    r = []
    for (a,b,n) in seqmatch(s1, s2):
        r.extend( (a+i,b+i) for i in xrange(n) )
    return r

def gmax(seq, key=lambda x:x, default=object()):
    (m,k0) = (default, 0)
    for x in seq:
        k1 = key(x)
        if m == default or k0 < k1: (m,k0) = (x,k1)
    return m

def path_diff(path1, path2):
    max_path_depth = 31
    path1_split = path1.split('/')
    path2_split = path2.split('/')
    assert(len(path1_split) <= max_path_depth and len(path2_split) <= max_path_depth)
    diff = 0
    for i in range(max_path_depth):
        if i >= len(path1_split) and i >= len(path2_split):
            break
        else:
            if i >= len(path1_split) or i >= len(path2_split) or path1_split[i] != path2_split[i]:
                diff += 1 << max_path_depth - i
    return diff


class LayoutSectionCluster:
    # blockgroups: [ blocks_doc1, blocks_doc2, ..., blocks_docn ]
    def __init__(self, id, blockgroups):
        self.id = id
        self.diffscore = None
        self.listscore = None
        self.weight = sum( sum( b.weight for b in blocks ) for blocks in blockgroups )
        self.weight_noanchor = sum( sum( b.weight_noanchor for b in blocks ) for blocks in blockgroups )
        self.weight_avg = self.weight_noanchor / float(len(blockgroups))
        self.blockgroups = blockgroups
        return
    
    def __repr__(self):
        return '<SC-%d (diff=%s, weight_noanchor=%d): %r>' %(self.id, self.diffscore, self.weight_noanchor, self.blockgroups[0][0].path)
    
    def calc_diffscore(self):
        (maxscore, score) = (0, 0)
        block_texts = [ ''.join( b.sig_text for b in blocks ) for blocks in self.blockgroups ]
        for (i,text0) in enumerate(block_texts):
            if not text0: continue
            for text1 in block_texts[i+1:]: # 0 <= j < i
                if not text1: continue
                maxscore += len(text0)+len(text1)
                score += diff_score(text0, text1)
        self.diffscore = score / float(lowerbound(maxscore, 1.0))
        return


def find_clusters(para_blocks):
    def uniq_path(blocks):
        r = []
        prev = None
        for b in blocks:
            if prev == None or b.path != prev:
                r.append(b.path)
            prev = b.path
        return r
    
    def find_common(seqs):
        s0 = None
        for s1 in seqs:
            if s0 == None:
                s0 = s1
            else:
                s0 = [ s0[i0] for (i0,i1) in find_lcs(s0, s1) ]
        return s0
    
    # obtain the common paths.
    common_paths = find_common([ uniq_path(blocks) for blocks in para_blocks ])
    # clusters = [ ( doc1_blocks1, doc2_blocks1, ..., docm_blocks1 ),
    #                ...
    #              ( doc1_blocksn, doc2_blocksn, ..., docm_blocksn ) ]
    clusters = zip(*[ retrieve_blocks(common_paths, blocks) for blocks in para_blocks ])
    # compare each cluster of text blocks.
    layout = []
    for blockgroups in clusters:
        if blockgroups[0]:
            layout.append(LayoutSectionCluster(len(layout), blockgroups))
    return layout


class LayoutCluster:
    def __init__(self, name, debug=0):
        self.name = name
        self.debug = debug
        self.pages = []
        self.score = 0
        self.pattern = None
        self.title_sectno = -1
        return
    
    def __repr__(self):
        return '<%s>' % self.name
    
    def add(self, page):
        self.pages.append(page)
        return
    
    def fixate(self, title_threshold):
        if len(self.pages) < 2: return
        layout = find_clusters([ p.blocks for p in self.pages ])
        if not layout: return
        # obtain the diffscores of this layout.
        for sect in layout:
            sect.calc_diffscore()
        # why log?
        self.score = log(len(self.pages)) * sum( sect.diffscore * sect.weight_avg for sect in layout )
        # discover main sections.
        self.pattern = [ (sect.diffscore, sect.diffscore*sect.weight_avg, sect.blockgroups[0][0].path) for sect in layout ]
        largest = gmax(layout, key=lambda sect: sect.diffscore*sect.weight_avg)
        if self.debug:
            for sect in layout:
                try:
                    logger.debug(' main: sect=%s, diffscore=%.2f, mainscore=%.2f, text=%s' % \
                        (sect, sect.diffscore, sect.diffscore*sect.weight_avg, 
                        ''.join( b.sig_text.encode('utf-8') for b in sect.blockgroups[0])))
                except UnicodeDecodeError:
                    logger.debug('%s, %r'%(b.sig_test, Traceback()))
        
        # discover title and main sections.
        logger.debug('Fixating: cluster=%r, pattern=%r, largest=%r' % (self, self.pattern, largest))
        title_sect_voted = {}
        for (pageno,p) in enumerate(self.pages):
            title_sect = None
            title_score = title_threshold
            logger.debug('%r: anchor_strs=%r' % (p, p.anchor_strs))
            # if anchor strings are available, compare them to the section texts.
            if p.anchor_strs:
                for i in xrange(largest.id):
                    sect = layout[i]
                    title = ''.join( b.sig_text.encode('utf-8') for b in sect.blockgroups[pageno] )
                    if not title: continue
                    score = max( dice_coeff(rt, title) for rt in p.anchor_strs if rt ) * sect.diffscore
                    logger.debug(' title: sect=%s, score=%.2f, title=%r' % (sect, score, title))
                    if title_score < score:
                        (title_sect, title_score) = (sect, score)
            
            # otherwise, use a fallback method.
            if not title_sect and 1 < len(layout):
                largest_text = ''.join( b.sig_text.encode('utf-8') for b in largest.blockgroups[pageno] )
                #logger.debug('FALLBACK:', largest_text[:50])
                title_score = 1.0
                for i in xrange(largest.id):
                    sect = layout[i]
                    title = ''.join( b.sig_text.encode('utf-8') for b in sect.blockgroups[pageno] )
                    if not title: continue
                    score = titlability(title, largest_text) * sect.diffscore
                    logger.debug(' sect=%s, score=%.2f, title=%r' % (sect, score, title))
                    if title_score < score:
                        (title_sect, title_score) = (sect, score)
            
            if title_sect not in title_sect_voted: title_sect_voted[title_sect] = 0
            title_sect_voted[title_sect] += 1
            logger.debug('title_sect=%r' % title_sect)
                
        (title_sect, dummy) = gmax(title_sect_voted.iteritems(), key=lambda (k,v): v)
        if title_sect:
            self.title_sectno = title_sect.id
        else:
            self.title_sectno = -1
        return
    
    def dump(self):
        res = '#%f%r\n'%(self.score, self)
        for p in self.pages:
            res += '#\t%r%r\n'%(p.name, p.url)
        res += '(%f, %r, %d, %r)\n'%(self.score, self.name, self.title_sectno, self.pattern)
        res += '\n'
        return res


class HTMLPage:
    
    def __init__(self, name, tree, url):
        self.name = name
        self.url = url
        self.blocks = get_textblocks(tree)
        self.weight = sum( b.weight for b in self.blocks )
        #self.weight_noanchor = sum( b.weight_noanchor for b in self.blocks )
        self.anchor_strs = []
        self.add_anchor_strs()
        return
    
    def __repr__(self):
        return '<%s>' % self.name
    
    def add_anchor_strs(self):
        page = Page().one({'url_hash': self.name})
        if not page: return
        self.anchor_strs.extend([sigchars(anchor['name']) for anchor in page.anchors if sigchars(anchor['name'])])
        return
    
class LayoutAnalyzer:
    
    def __init__(self, sample_page=None, debug=0):
        self.pages = {}
        self.debug = debug
        self.sample_page = sample_page
        self.most_match = None
        return
    
    def add_tree(self, name, tree, url):
        page = HTMLPage(name, tree, url)
        self.pages[name] = page
        return len(self.pages)
    
    def analyze(self, cluster_threshold=0.97, title_threshold=0.6, max_sample=0, verbose=True):
        logger.debug('Clustering %d files with threshold=%f...' % (len(self.pages), cluster_threshold))
        clusters = []
        keys = self.pages.keys()
        for (urlno, url1) in enumerate(keys):
            page1 = self.pages[url1]
            logger.debug(' %d: %r' % (urlno, page1))
            if verbose:
                stderr.write(' %d: ' % urlno)
            # search from the smallest cluster (not sure if this helps actually...)
            # clusters.sort(key=lambda c: len(c.pages))
            clusters.sort(key=lambda c: min([ path_diff(dirname(page1.name), dirname(x.name)) for x in c.pages ]))
            old_sim = 0
            for c0 in clusters:
                has_match = False
                if max_sample:
                    random.seed()
                    pages = c0.pages[:]
                    random.shuffle(pages)
                    pages = pages[:max_sample]
                else:
                    pages = c0.pages
                for page2 in pages:
                    layout = find_clusters([ page1.blocks, page2.blocks ])
                    total_weight = sum( c.weight for c in layout )
                    sim = total_weight / lowerbound(float(page1.weight + page2.weight), 1)
                    logger.debug('    sim=%.3f (%d): %r' % (sim, total_weight, page2))
                    if verbose:
                        stderr.write('.'); stderr.flush()
                    if self.sample_page and sim > old_sim and page2.name == self.sample_page:
                        old_sim = sim
                        has_match = True
                    if sim < cluster_threshold: break
                else:
                    logger.debug('joined: %r' % c0)
                    if has_match:
                        self.most_match = c0
                    c0.add(page1)
                    break
            else:
                c0 = LayoutCluster(url1, debug=self.debug)
                c0.add(page1)
                logger.debug('formed: %r' % c0)
                clusters.append(c0)
        stderr.write('Fixating')
        for c in clusters:
            c.fixate(title_threshold)
            stderr.write('.'); stderr.flush()
        clusters.sort(key=lambda c: c.score, reverse=True)
        return clusters


class PageFeeder:
    
    def __init__(self, analyzer, debug=0):
        self.analyzer = analyzer
        self.debug = debug
        self.dic = {}
        return
    
    # dirtie
    def accept_url(self, url):
        return url
    
    def inject_url(self, url):
        return True
    
    def feed_page(self, version):
        page = Page().one({'_id': version.page})
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
            logger.error(Traceback())
            return
        n = self.analyzer.add_tree(name, tree, effective_url)
        logger.debug('Added: %d: %s, %s' % (n, name, url))
        return
    
    def close(self):
        return

def generate_link_pattern(pages):
    template = BaseTemplate(tolerance=1)
    #urls = ['http://hi.baidu.com/xzkcz/blog/item/1.2.html','http://hi.baidu.com/xzkcz/blog/item/0.html','http://hi.baidu.com/xzkcz/blog/item/2.html','http://hi.baidu.com/xzkcz/blog/item/3.html']
    template.learn(p.url for p in pages)
    return template.pattern((p.url for p in pages))

def save_analysis_result(identifier, fullpac):
    site = Site().one({'url_hash': identifier})
    if not site: return
    site.pattern = fullpac if isinstance(fullpac, unicode) else fullpac.decode('utf-8')
    site.save()
    
def Analyzer(identifier, sample_page=None, cluster_threshold=0.74, title_threshold=0.6, score_threshold = 100, max_sample=5, debug=False):
    analyzer = LayoutAnalyzer(sample_page, debug=debug)
    dumpfile = ''
    for box in PageSandbox().find({'identifier':identifier, 'mixed': 0, 'analyzer': 1}):
        feeder = PageFeeder(analyzer, debug=debug)
        for version in box.page_versions:
            version = PageVersion().one({'_id':version})
            if not version: continue
            feeder.feed_page(version)
        feeder.close()
        box.delete()
    dumpfile += '### cluster_threshold=%f\n' % cluster_threshold
    dumpfile += '### title_threshold=%f\n' % title_threshold
    dumpfile += '### pages=%d\n' % len(analyzer.pages)
    dumpfile += '\n'
    ares = analyzer.analyze(cluster_threshold, title_threshold, max_sample)
    if analyzer.most_match: item_pattern = analyzer.most_match
    elif ares: item_pattern = ares[0]
    else: return
    #item_pattern.dump()
    if not item_pattern: return
    
    i = 0
    for c in ares:
        if c.pattern and score_threshold <= c.score:
            dumpfile += c.dump()
    logger.debug(dumpfile)
    pac = {'url':'','title':'','content':''}
    pac['url'] = generate_link_pattern(item_pattern.pages)
    if item_pattern.title_sectno > 0: pac['title'] = item_pattern.pattern[item_pattern.title_sectno]
    if not item_pattern.pattern: return
    tmp = sorted(item_pattern.pattern, key=operator.itemgetter(1))
    for t in tmp[::-1]:
        if t[2]!='title':
            pac['content'] = t
            break
    tmp = []
    del tmp
    save_analysis_result(identifier, dumpfile)