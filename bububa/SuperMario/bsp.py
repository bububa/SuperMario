#!/usr/bin/env python
# encoding: utf-8

import sys
import os
import re
import md5
import logging
from urllib import urlencode
from urlparse import urlsplit, urljoin, urlparse, urlunparse
from eventlet import db_pool, coros, util  # for io/async co-routines
from eventlet.api import with_timeout
from bububa.SuperMario.utils import URL
from bububa.SuperMario.Mario import Mario, MarioBatch
from bububa.SuperMario.utils import Traceback

stderr = sys.stderr

logger = logging.getLogger("bsp")
handler = logging.StreamHandler()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

class BSP:
    
    def __init__(self):
        self.support_bsps = [(r'^http://spaces.msn.com/members/([^^].*)', ('login.live.com', 'spaces.live.com', 'my.msn.com', 'profile.live.com', 'signup.alerts.live.com'), 'live', 'utf-8'),
                            (r'^http://spaces.msn.com/([^^].*)', ('members', 'login.live.com', 'spaces.live.com', 'my.msn.com', 'profile.live.com', 'signup.alerts.live.com'),'live', 'utf-8'),
                            (r'^http://([^^].*?).spaces.msn.com', ('www', 'login.live.com', 'spaces.live.com', 'my.msn.com', 'profile.live.com', 'signup.alerts.live.com'),'live', 'utf-8'),
                            (r'^http://([^^].*?).home.services.spaces.live.com', ('www', 'login.live.com', 'spaces.live.com', 'my.msn.com', 'profile.live.com', 'signup.alerts.live.com'), 'live', 'utf-8'),
                            (r'^http://([^^].*?).spaces.live.com', ('www','login.live.com', 'spaces.live.com', 'my.msn.com', 'profile.live.com', 'signup.alerts.live.com'), 'live', 'utf-8'),
                            (r'^http://([^^].*?).ycool.com', ('www', 'blog', 'account', 'my', 'rss', 'foto'), 'ycool', 'utf-8'),
                            (r'^http://([^^].*?).yculblog.com', ('www', 'blog', 'account', 'my', 'rss', 'foto'), 'ycool', 'utf-8'),
                            (r'^http://hexun.com/([^^].*)', (), 'hexun', 'gbk'),
                            (r'^http://blog.hexun.com/([^^].*)', (), 'hexun', 'gbk'), 
                            (r'^http://([^^].*?).blog.hexun.com', ('www',), 'hexun', 'gbk'),
                            (r'^http://([^^].*?).blogbus.com', ('www', 'tag', 'pindao', 'kanfa', 'tuijian', 'banjia', 'bloglink', 'marketing', 'epaper', 'log', 'home', 'app.home', 'feedsky', 'blogbus'), 'blogbus', 'utf-8'), 
                            (r'^http://blog.sohu.com/members/([^^].*)', (), 'sohu', 'gbk'),
                            (r'^http://([^^].*?).blog.sohu.com', ('www', 'v', 'ow'), 'sohu', 'gbk'),
                            (r'^http://www.blogcn.com/user[^^]*?/([^^].*)', ('www', 'mb', 'gz', 'bk', 'rz', 'bkgame', 'hd', 'e', 'ty', 'sys', 'sys1', 'sys2', 'faq', 'we', 'tclub', 'zt', 'sq', 'images'), 'blogcn', 'utf-8'), 
                            (r'^http://www.blogcn.com/blog/?u=([^^].*)', ('www', 'mb', 'gz', 'bk', 'rz', 'bkgame', 'hd', 'e', 'ty', 'sys', 'sys1', 'sys2', 'faq', 'we', 'tclub', 'zt', 'sq', 'images'), 'blogcn', 'utf-8'), 
                            (r'^http://www.blogcn.com/u/([^^].*)', ('www', 'mb', 'gz', 'bk', 'rz', 'bkgame', 'hd', 'e', 'ty', 'sys', 'sys1', 'sys2', 'faq', 'we', 'tclub', 'zt', 'sq', 'images'), 'blogcn', 'utf-8'),
                            (r'^http://userinfo.blogcn.com/([^^].*?).shtml', ('www', 'mb', 'gz', 'bk', 'rz', 'bkgame', 'hd', 'e', 'ty', 'sys', 'sys1', 'sys2', 'faq', 'we', 'tclub', 'zt', 'sq', 'images'), 'blogcn', 'utf-8'),
                            (r'^http://([^^].*?).blogcn.com', ('www', 'mb', 'gz', 'bk', 'rz', 'bkgame', 'hd', 'e', 'ty', 'sys', 'sys1', 'sys2', 'faq', 'we', 'tclub', 'zt', 'sq', 'images'), 'blogcn', 'utf-8'),
                            (r'^http://([^^].*?).cnblogs.com', ('life', 'beginer', 'cio', 'q', 'translate', 'gis', 'www', 'news', 'space', 'job', 'wz', 'zzk', 'book', 'tag', 'kb', 'zhengtu', 'emag', 'home', 'beijing', 'team', 'chengdu', 'chongqing', 'sharepoint', 'comment', 'topic', 'java', 'delphi', 'sap', 'testing', 'expert', 'mvpgroup', 'press', 'pick', 'quoted', 'service', 'job2', 'all'), 'cnblogs', 'utf-8'), 
                            (r'^http://www.cnblogs.com/([^^].*?)/', ('life', 'beginer', 'cio', 'q', 'translate', 'gis', 'www', 'news', 'space', 'job', 'wz', 'zzk', 'book', 'tag', 'kb', 'zhengtu', 'emag', 'home', 'beijing', 'team', 'chengdu', 'chongqing', 'sharepoint', 'comment', 'topic', 'java', 'delphi', 'sap', 'testing', 'expert', 'mvpgroup', 'press', 'pick', 'quoted', 'service', 'job2', 'all'), 'cnblogs', 'utf-8'), 
                            (r'^http://([^^].*?).blog.163.com', ('www', 'pp', 'reg', 'reg.163.com'), '163', 'utf-8'), 
                            (r'^http://blog.163.com/([^^].*?)/blog', ('search', 'services', 'www', 'reg.163.com'), '163', 'utf-8'),
                            (r'^http://i.mop.com/([^^].*)', (), 'mop', 'gbk'),  
                            (r'^http://blog.tianya.cn/[^^]*?blogname=([^^].*)', ('www1', 'www2', 'www', 'club', 'id'), 'tianya', 'gbk'), 
                            (r'^http://([^^].*?).blog.tianya.cn', ('www1', 'www2', 'www', 'club', 'id'), 'tianya', 'gbk'),
                            (r'^http://([^^].*?).tianyablog.com', ('www1', 'www2', 'www', 'club', 'id'), 'tianya', 'gbk'), 
                            (r'^http://[^^]*?.tianyablog.com/[^^]*?blogname=([^^].*)', ('www1', 'www2', 'www', 'club'), 'tianya', 'gbk'),
                            (r'^http://blog.sina.com.cn/m/([^^].*)', (), 'sina', 'utf-8'), 
                            (r'^http://blog.sina.com.cn/u/', (), 'sina', 'utf-8'), 
                            (r'^http://blog.sina.com.cn/([^^].*)', ('main_v5', ), 'sina', 'utf-8'),
                            (r'^http://hi.baidu.com/sys/checkuser/([^^].*)', (), 'baidu', 'gbk'),
                            (r'^http://hi.baidu.com/([^^].*)', (), 'baidu', 'gbk'),]
        self.pac = {'live':{'type':'regx', 'url':r'http://[^^].*?.spaces.live.com/blog/cns!([^^].*?).entry', 'title':r'<h4 class="beTitle"[^^].*?>([^^].*?)</h4>', 'content':r'<div id="msgcns![^^].*?" class="bvMsg">([^^].*?)</div>', 'tags':r'<div class="footerLinks"><nobr id="blogDT">[^^].*?Blog it</a></nobr>&nbsp;| <nobr dir="ltr">([^^].*?)</nobr></div>'},
                    'ycool':{'type':'regx', 'url':r'http://[^^].*?.ycool.com/post.\d+.html', 'title':r'<a href="[^^].*?" class="post_title" rel="follow">([^^].*?)</a>', 'content':r'<div class="post_content">([^^].*?)</div>', 'tags':r'<div class="post_tags">([^^].*?)</div>'},
                    'blogcn':{'type':'regx', 'url':r'http://[^^].*?.blogcn.com/diary,\d+.shtml', 'title':r'<font class="diary_title">([^^].*?)</font>', 'content':r'<div class="css_blogcn_dcotent">([^^].*?)</div>', 'tags':r'<div class="css_blogcn_dtag">([^^].*?)</div>'},
                    'cnblogs':{'type':'regx', 'url':r'http://www.cnblogs.com/[^^].*?/archive/\d{4}/\d{2}/\d{2}/\d+.html', 'title':r'<h1 class = "postTitle">([^^].*?)</h1>', 'content':r'<div class="postBody">([^^].*?)</div>', 'tags':r'<div id="EntryTag">([^^].*?)</div>'},
                    'hexun':{'type':'regx', 'url':r'http://[^^].*?.blog.hexun.com/\d+_d.html', 'title':r'<div class="ArticleTitle"><span class="ArticleTitleText">([^^].*?)</span>', 'content':r'<div class="ArticleBlogText">([^^].*?)</div>', 'tags':r'<div class="ArticleTag">([^^].*?)</div>'},
                    '163':{'type':'regx', 'url':r'http://[^^].*?.blog.163.com/blog/static/[^^].*?/', 'title':r'<p[^^].*?id="blogtitle_[^^].*?>([^^].*?)</p>', 'content':r'<div[^^].*?id="blogtext_[^^].*?>([^^].*?)</div>', 'tags':r''},
                    'tianya':{'type':'regx', 'url':r'http://blog.tianya.cn/blogger/post_show.asp?BlogID=\d+&PostID=\d+idWriter=0&Key=0', 'title':r'<div class="vtitle">([^^].*?)</div>', 'content':r'<DIV class=vcon>([^^].*?)</DIV>', 'tags':r''},
                    'sina':{'type':'regx', 'url':r'http://blog.sina.com.cn/s/blog_[^^].*?.html', 'title':r'<div class="articleTitle"><b[^^].*?>([^^].*?)</b>', 'content':r'<div class="articleContent" id="articleBody">([^^].*?)</div>', 'tags':r'<div class="articleTag">[^^].*?标签：([^^].*?)</td>'},
                    'blogbus':{'type':'regx', 'url':r'http://[^^].*?.blogbus.com/logs/[^^].*?.html', 'title':r'<div class="postHeader">[^^].*?<h2>([^^.*?])</h2>', 'content':r'<div class="postBody">[^^].*?<p class="cc-lisence" style="line-height:180%;">[^^].*?</p>([^^].*?)</div>', 'tags':r'<div class="postHeader">[^^].*?<span class="tags">([^^.*?])</span></h3>'},
                    'sohu':{'type':'regx', 'url':r'http://[^^].*?.blog.sohu.com/\d+.html', 'title':r'<div class="item-title">[^^].*?<h3>[^^].*?&nbsp;\|&nbsp;([^^].*?)</h3>', 'content':r'<div class="item-content" id="main-content">([^^].*?)</div>', 'tags':r'<div class="item-content">标签：<span id="tagsBox">([^^].*?)</span>'},
                    'baidu':{'type':'regx', 'url':r'http://hi.baidu.com/[^^].*?/blog/item/[^^].*?.html', 'title':r'<div id="m_blog"[^^].*?<div class="tit">([^^].*?)</div>', 'content':r'<div id="blog_text" class="cnt"  >([^^].*?)</div>', 'tags':r''}
        }
    
    def normalize(self, url):
        url = URL.normalize(url)
        tmp = url.split('?')
        for b in self.support_bsps:
            pattern = re.compile(b[0], re.I)
            res = pattern.findall(url)
            if res:
                name = self.normalizeName(res[0])
                if len(tmp)>1 and b[2] in tmp[1] and 'http' in tmp[1] or name in b[1]: continue
                if b[2] == 'tianya': return self.validTianya(name, b[2])
                if b[2] == 'ycool': return self.validYcool(name, b[2])
                if b[2] == 'blogcn': return self.validBlogcn(name, b[2])
                if b[2] == '163': return self.valid163(name, url, b[2])
                if b[2] == 'cnblogs': return self.validCnblogs(name, b[2])
                if b[2] == 'sina': return self.validSina(url, b[2])
                if b[2] == 'live': return self.validLive(name, url, b[2])
                if b[2] == 'blogbus': return self.validBlogbus(name, b[2])
                if b[2] == 'baidu': return self.validBaidu(name, url, b[2])
                if b[2] == 'hexun': return self.validHexun(name, b[2])
                if b[2] == 'sohu': return self.validSohu(name, b[2])
                if b[2] == 'mop': return self.validMop(name, b[2])
        return None
    
    def get_pac(self, url):
        bsp_info = self.normalize(url)
        if not bsp_info: return None
        if bsp_info[2] not in self.pac: return None
        self.pac[bsp_info[2]]['identifier'] = md5.new(bsp_info[1]).hexdigest()
        return self.pac[bsp_info[2]]
    
    def get_entry_pac(self, url):
        for sp, pac in self.pac.items():
            pattern = re.compile(pac['url'])
            if pattern.match(url):
                return pac
        return None
        
    def validTianya(self, name, sp):
        if not name: return None
        name = name.split('&')[0]
        if name and name not in ('www1', 'www2', 'id'): 
            return (name, 'http://blog.tianya.cn/blogger/view_blog.asp?BlogName=%s'%name, sp)
        return None
    
    def validYcool(self, name, sp):
        return (name, 'http://%s.ycool.com/'%name, sp)
    
    def validBlogcn(self, name, sp):
        return (name, 'http://%s.blogcn.com/index.shtml'%name, sp)
    
    def valid163(self, name, url, sp):
        if '@' in url:
            return (name, 'http://blog.163.com/%s/'%name, sp)
        else:
            return (name, 'http://%s.blog163.com/'%name, sp)
    
    def validCnblogs(self, name, sp):
        if '.aspx' not in name and '.html' not in name:
            return (name, 'http://www.cnblogs.com/%s/'%name, sp)
        return None
    
    def validSina(self, url, sp):
        username=''
        if 'blog.sina.com.cn/s/' in url or 'http://blog.sina.com.cn/lm/' in url: return None
        elif 'blog.sina.com.cn/m/' in url:
            pattern = re.compile(r'http://blog.sina.com.cn/m/([^^].*)', re.I)
            res = pattern.findall(url)
            if not res: return None
            username = self.normalizeName(res[0])
        elif 'blog.sina.com.cn/u/' in url:
            mario = Mario()
            response = mario.get(url)
            if not response or not response.body: return None
            html = response.body
            pattern = re.compile(r'uhost : "([^^].*?)"')
            res = pattern.findall(html)
            if not res: return None
            username = res[0]
        elif 'http://blog.sina.com.cn/' in url:
            pattern = re.compile(r'http://blog.sina.com.cn/([^^].*)', re.I)
            res = pattern.findall(url)
            if not res: return None
            username = self.normalizeName(res[0])
            if username in ('main_v5', ): username=None
        if username:
            return (username, 'http://blog.sina.com.cn/%s/'%username, sp)
        return None
    
    def validLive(self, name, url, sp):
        if 'UploadFile' in url: return None
        return (name, 'http://%s.spaces.live.com/'%name, sp)
    
    def validBlogbus(self, name, sp):
        return (name, 'http://%s.blogbus.com/'%name, sp)
    
    def validBaidu(self, name, url, sp):
        username=''
        if 'http://hi.baidu.com/sys/checkuser/' in url:
            pattern = re.compile(r'http://hi.baidu.com/sys/checkuser/([^^].*)', re.I)
            res = pattern.findall(url)
            if not res: return None
            username = self.normalizeName(res[0])
        elif 'http://hi.baidu.com' in url and '/blog/item/' not in url:
            pattern = re.compile(r'http://hi.baidu.com/([^^].*)', re.I)
            res = pattern.findall(url)
            if not res: return None
            username = self.normalizeName(res[0])
        if username: 
            return (username, 'http://hi.baidu.com/%s/'%username, sp)
        return None
    
    def validHexun(self, name, sp):
        return (name, 'http://%s.blog.hexun.com/'%name, sp)
    
    def validSohu(self, name, sp):
        return (name, 'http://%s.blog.sohu.com/'%name, sp)
    
    def validMop(self, name, sp):
        return (name, 'http://i.mop.com/%s/blog/'%name, sp)
    
    def normalizeName(self, username):
        if username.startswith('/'): username = username[1:]
        return username.split('/')[0].split('?')[0].split('#')[0]


class Friends:
    
    def __init__(self, url, page=None, debug=False):
        self.url = URL.normalize(url)
        self.page = page
        if not page:
            mario = Mario()
            response = mario.get(self.url)
            if response and response.body:
                self.page = response.body
        self.debug = debug
        bsp = BSP()
        self.bsp_info = bsp.normalize(url)
    
    def get(self):
        if not self.bsp_info:
            logger.debug('Not a valid bsp')
            return None
        if not self.page:
            logger.debug("Cant't fetch content.")
            return None
        html = self.page
        username, homepage, sp  = self.bsp_info
        links_url = None
        if sp not in ('tianya', 'ycool', 'blogcn', '163', 'cnblogs', 'sina', 'live', 'blogbus', 'baidu', 'hexun', 'sohu'):
            return None
        if sp == 'sohu':
            pattern = re.compile("var _ebi = '([^^].*?)'")
            res = pattern.findall(html)
            if res: links_url = 'http://blog.sohu.com/sff/links/%s.html'%res[0]
        elif sp == '163':
            pattern = re.compile("hostName     : '([^^].*?)'")
            hostNameRes = pattern.findall(html)
            if hostNameRes: hostName = hostNameRes[0]
            pattern = re.compile("dataDigest	  : '([^^].*?)'")
            dataDigest = pattern.findall(html)
            if dataDigest: dataDigest = dataDigest[0]
            if hostNameRes and dataDigest: 
                link = 'http://%s.blog.163.com/friends/dwr/call/plaincall/UserBean.getFriends.dwr'%hostName
                mario = Mario()
                body = [('callCount', '1'), ('scriptSessionId', '${scriptSessionId}561'), ('c0-scriptName', 'UserBean'), ('c0-methodName','getFriends'), ('c0-id', 0), ('c0-param0', 'boolean:false'), ('c0-param1', 'number:0'), ('c0-param2', 'number:0'), ('c0-param3', 'number:20'), ('batchId', 0),]
                response = mario.get(link, body=urlencode(body))
                if response and response.body: html = response.body
        elif sp == 'baidu':
            links_url = urljoin(homepage, 'friends')
        elif sp == 'hexun':
            html = ''
            friend_links = []
            page = 1
            results = []
            def callback(response):
                if response and response.body: results.append(response.body)
            while True:
                mario = Mario()
                response = mario.get('http://hexun.com/%s/%d/t0/friends.html'%(username, page))
                if not response or not response.body: break;
                friendsPage = response.body
                pattern = re.compile('<!--  朋友列表:开始  -->[^^]*?<!--  朋友列表:结束  -->')
                if friendsPage: dom = pattern.findall(friendsPage)
                if not friendsPage or not dom: break
                pattern = re.compile('<div class="FriendTableList_2_1_1"><a href="/([^^].*?)/default.html"', re.I)
                ids = pattern.findall(dom[0])
                if not ids: break
                has_friend_link = False
                results = []
                mario = MarioBatch(callback=callback)
                for friend_id in ids:
                    mario.add_job('http://hexun.com/%s/default.html'%friend_id)
                mario(5)
                if not results: break
                pattern = re.compile('blogname=([^^].*?)&preview=', re.I)
                for f in results:
                    if not f: continue
                    res = pattern.findall(f)
                    if not res: continue
                    friend_links.append('<a href="http://%s.blog.hexun.com/">link</a>'%res[0])
                    has_friend_link = True
                if not has_friend_link: break
                page += 1
            html = ','.join(friend_links)
        elif sp == 'blogcn':
            bsp = BSP()
            nu = bsp.normalize(homepage)
            if nu!=homepage:
                mario = Mario()
                response = mario.get(nu[1])
                if response and response.body: html = response.body
        if links_url:
            html = '' 
            mario = Mario()
            response = mario.get(links_url)
            if response and response.body:
                html = response.body
        return self.parser(html, sp, homepage)
    
    def parser(self, html, sp, homepage):
        if not html: return None
        links = []
        if sp == 'baidu':
            pattern = re.compile('nameEnc: "([^^].*?)"')
            username = pattern.findall(html)
            if not username: return None
            link = 'http://frd.baidu.com/api/friend.getlist?un=%s'%username[0]
            mario = Mario()
            response = mario.get(link)
            if not response or not response.body: return None
            pattern = re.compile('\["([^^].*?)","[^^].*?","[^^].*?","[^^].*?",\d+,"[^^].*?",\d+,\d+\]')
            names = pattern.findall(response.body)
            if not names: return None
            bsp = BSP()
            for n in names:
                u = bsp.normalize('http://hi.baidu.com/sys/checkuser/%s'%n)
                if u and u[1] != homepage and u[1] not in links:
                    links.append(u)
        elif sp == 'sohu':
            pattern = re.compile('"link" : "([^^].*?)"', re.I)
            urls = pattern.findall(html)
            bsp = BSP()
            for url in urls:
                r = bsp.normalize(url)
                if r and r[1] != homepage and r[1] not in links:
                    links.append(r[1])
        elif sp == '163':
            pattern = re.compile('.userName="([^^].*?)"')
            usernames = pattern.findall(html)
            links = []
            bsp = BSP()
            for u in usernames:
                if not u: continue
                link = bsp.valid163(u, 'http:%s.blog.163.com/'%u, '163')
                if link and link[1] and link[1] not in links: links.append(link[1])
        else:
            bsp = BSP()
            for link, title in URL.link_title(html, homepage):
                if not link:
                    continue
                r = bsp.normalize(link)
                if r and r[1] != homepage and r[1] not in links:
                    links.append(r[1])
        return links


class Avatar:
    
    @staticmethod
    def get(url, html):
        url = URL.normalize(url)
        bsp = BSP()
        bsp_info = bsp.normalize(url)
        if not bsp_info: return None
        username, homepage, sp = bsp_info
        mario = Mario()
        if sp == 'sohu':
            pattern = re.compile("var _ebi = '([^^].*?)'")
            res = pattern.findall(html)
            if not res: return None
            response = mario.get("http://blog.sohu.com/action/ebi_%s-m_view-type_profile/widget/"%res[0])
            if not response or not response.body: return None
            pattern = re.compile('<div id="profile_photo">[^^]*?<img src="([^^].*?)"')
            res = pattern.findall(response.body)
            if not res: return None
            return res[0]
        elif sp == '163':
            pattern = re.compile("hostName     : '([^^].*?)'")
            hostName = pattern.findall(html)
            if hostName: hostName = hostName[0]
            pattern = re.compile("dataDigest	  : '([^^].*?)'")
            dataDigest = pattern.findall(html)
            if dataDigest: dataDigest = dataDigest[0]
            if not hostName or not dataDigest: return None
            response = mario.get('http://ud3.blog.163.com/%s/%s/modi=1208265646323&mid=0&tid=0&pdm=1/prev.js'%(hostName, dataDigest))
            if not response or not response.body: return None
            pattern = re.compile('<img class=[^^]*?src=[^^]*?"([^^].*?)"')
            res = pattern.findall(response.body)
            if res: return res[0][:-1]
            response = mario.get('http://blog.163.com/%s/profile/'%hostName)
            if not response or not response.body: return None
            pattern = re.compile('<img class="bd01 g_img_00 g_c_hand" src="([^^].*?)"')
            res = pattern.findall(response.body)
            if not res: return None
            return res[0]
        elif sp == 'blogcn':
            response = mario.get(homepage)
            if not response or not response.body: return None
            pattern = re.compile('var[^^]*?blogusername="([^^].*?)"')
            res = pattern.findall(response.body)
            if not res:return None
            response = mario.get('http://userinfo.blogcn.com/%s.shtml'%res[0])
            if not response or not response.body: return None
            pattern = re.compile('<img class="top-5px" src="([^^].*?)"')
            res = pattern.findall(response.body)
            if not res: return None
            return res[0]
        elif sp == 'ycool':
            response = mario.get(homepage)
            if not response or not response.body: return None
            pattern = re.compile('<a href="http://www.ycool.com/space.php?uid=([^^].*?)"')
            res = pattern.findall(response.body)
            if not res:return None
            return 'http://ug.ycstatic.com/avatar/%sx96.jpg'%res[0]
        elif sp == 'hexun':
            response = mario.get(homepage)
            if not response or not response.body: return None
            pattern = re.compile('<div id="master_ptoto_1">[^^]*?<script src=\'([^^].*?)\'>')
            res = pattern.findall(response.body)
            if not res:return None
            response = mario.get(res[0])
            if not response or not response.body: return None
            pattern = re.compile("<img src='([^^].*?)'")
            res = pattern.findall(response.body)
            if not res:return None
            return res[0]
        elif sp == 'live':
            response = mario.get(homepage)
            if not response or not response.body: return None
            pattern = re.compile('<div class="cxp_ic_tile_clip"[^^]*?<img[^^]*?src="([^^].*?)"')
            res = pattern.findall(response.body)
            if not res:return None
            response = mario.get(urljoin(homepage, 'recent/'))
            if not response or not response.body: return None
            pattern = re.compile('<div class="cxp_ic_tile_clip"[^^]*?<img[^^]*?src="([^^].*?)"')
            res = pattern.findall(response.body)
            if not res:return None
            return res[0]
        elif sp == 'blogbus':
            response = mario.get(homepage)
            if not response or not response.body: return None
            pattern = re.compile('<img class="avatar" src="([^^].*?)"')
            res = pattern.findall(response.body)
            if not res:return None
            return res[0]
        elif sp == 'sina':
            response = mario.get(homepage)
            if not response or not response.body: return None
            pattern = re.compile('<div id="userImage">[^^]*?<img[^^]*?src="([^^].*?)"')
            res = pattern.findall(response.body)
            if res: return res[0]
            pattern = re.compile('<div class="image">[^^]*?<img[^^]*?src="([^^].*?)"')
            res = pattern.findall(response.body)
            if not res: return None
            return res[0]
        elif sp == 'tianya':
            response = mario.get(homepage)
            if not response or not response.body: return None
            pattern = re.compile('<BloggerMemsList>[^^]*?<a href="http://www.tianya.cn/browse/listwriter.asp\?vwriter=([^^].*?)&idWriter=0&Key=0"[^^]*?</a>')
            res = pattern.findall(response.body)
            if not res: return None
            response = mario.get('http://my.tianya.cn/mytianya/ListWriterNew.asp?vwriter=%s'%res[0])
            if not response or not response.body: return None
            pattern = re.compile('<img onload="[^^]*?src="([^^].*?)"')
            res = pattern.findall(response.body)
            if not res: return None
            return res[0]
        elif sp == 'baidu':
            response = mario.get(homepage)
            if not response or not response.body: return None
            pattern = re.compile('<div class="portrait">[^^]*?<img src="([^^].*?)"')
            res = pattern.findall(response.body)
            if not res: return None
            return res[0]
        elif sp == 'mop':
            response = mario.get(homepage)
            if not response or not response.body: return None
            pattern = re.compile('<div[^^]*?class="fava_box"[^^]*?<img[^^]*?src="([^^].*?)"')
            res = pattern.findall(response.body)
            if not res: return None
            return res[0]