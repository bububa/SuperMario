#!/usr/bin/env python
"""ExtMainText: Parses HTML and filters non main text parts.

http://www.elias.cn/En/ExtMainText

ExtMainText parses a HTML document, keeps only html about main text, and 
filter advertisements and common menus in document.

Such module could help search engine focus on most valuable part of html
documents, and help page monitors and extractors just pay attension on most 
meaningful part.

The current implementation bases on the measure of html tag density, and 
determine the threshold according to historical experience. The original 
algorithm comes from http://www.xd-tech.com.cn/blog/article.asp?id=59

The "if __name__ == '__main__'" part of such module can be a usage sample of 
extmaintext.

Here, have some legalese:

Copyright (c) 2008, Elias Soong

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

  * Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.

  * Redistributions in binary form must reproduce the above
    copyright notice, this list of conditions and the following
    disclaimer in the documentation and/or other materials provided
    with the distribution.

  * Neither the name of the the Beautiful Soup Consortium and All
    Night Kosher Bakery nor the names of its contributors may be
    used to endorse or promote products derived from this software
    without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE, DAMMIT.

"""

__author__ = "Elias Soong (elias.soong@gmail.com)"
__version__ = "0.1a"
__copyright__ = "Copyright (c) 2008 Elias Soong"
__license__ = "New-style BSD"

import BeautifulSoup

def extMainText(html, threshold = 0.03, debug = False):
    """
    Parses HTML and filters non main text parts.
    Return: html part of main text
    """
    soup = BeautifulSoup.BeautifulSoup(html)
    soup = soup.body
    countDic = calcDensity(soup)
    if debug:
        print countDic
        print "======"
    maxSoup, textLen = getMainText(countDic, threshold)
    return unicode(maxSoup)

def getMainText(countDic, threshold):
    """
    Get the longest html part with tag density smaller than threshold according 
    to density dictionary.
    Return: (soup object, max text length with small enough tag density)
    """
    dens, tagNo, textLen, soup = countDic['self']
    if dens <= threshold:
        maxSoup = soup
        maxTextLen = textLen
    else:
        maxSoup = BeautifulSoup.BeautifulSoup("")
        maxTextLen = 0
    if countDic.has_key('child'):
        for childDic in countDic['child']:
            soup, textLen = getMainText(childDic, threshold)
            if textLen > maxTextLen:
                maxSoup = soup
                maxTextLen = textLen
    return (maxSoup, maxTextLen)

def calcDensity(soup):
    """
    Count the number of html tags and the length of pure text information in a
    soup entity.
    Return: {'self': (tag density, number of tags, length of pure text, soup object), 
    'child': list of count dics for child entities }
    """
    uni = unicode(soup)
    if isinstance(soup, BeautifulSoup.NavigableString):
        if uni.startswith("<!--"):
            return {'self': (0.0, 0, 0, soup)}
        return {'self': (0.0, 0, len(uni), soup)}
    if soup.name in ("script", "style"):
        return {'self': (1.0, 0, 0, BeautifulSoup.BeautifulSoup(""))}
    countTagNo = 1   # This is the current tag.
    countTextLen = 0
    dicList = []
    for content in soup.contents:
        dic = calcDensity(content)
        dicList.append(dic)
        tagNo, textLen = dic['self'][1:3]
        countTagNo += tagNo
        countTextLen += textLen
    density = countTextLen != 0 and float(countTagNo) / countTextLen or 1.0
    return {'self': (density, countTagNo, countTextLen, soup), 'child': dicList}

if __name__ == '__main__':
    import sgmllib
    class html2txt(sgmllib.SGMLParser):
        """
        html to text converter without encoding transition.
        """
      
        def reset(self):
            """
            reset() --> initialize the parser
            """
            sgmllib.SGMLParser.reset(self)
            self.pieces = []
    
        def handle_data(self, text):
            """
            handle_data(text) --> appends the pieces to self.pieces
            handles all normal data not between brackets "<>"
            """
            self.pieces.append(text)
    
        def handle_entityref(self, ref):
            """
            called for each entity reference, e.g. for "&copy;", ref will be "copy"
            Reconstruct the original entity reference.
            """
            if ref=='amp':
                self.pieces.append("&")
    
        def output(self):
            """
            Return processed HTML as a single string
            """
            return " ".join(self.pieces)
    
        def convert_charref(self, name):
            """
            Convert character reference, override the function in sgmllib.
            We do this to fix an encoding bug of it.
            """
            try:
                n = int(name)
            except ValueError:
                return
            if not 0 <= n <= 127:
                return
            return self.convert_codepoint(n)
        
    import sys, os
    if len(sys.argv) < 2:
        print """Extract the main text of a html document.
  Usage: python ExtMainText.py %HTML_FILE_NAME% %THRESHOLD%
         %HTML_FILE_NAME% is the file name of target html document
         %THRESHOLD% the tag density threshold (Default: 0.03)
  Suggest: English document could choose %THRESHOLD% as 0.02-0.03
           Chinese document could choose %THRESHOLD% as 0.01-0.04
           But you should find suitable threshold for specific site yourself!
        """
    else:
        argv = sys.argv
        argv.extend((None, None))
        fileName, threshold, debug = sys.argv[1:4]
        threshold = threshold == None and 0.03 or float(threshold)
        debug = debug != None and True or False
        if os.path.exists(fileName):
            f = open(fileName, 'r')
            html = f.read()
            f.close()
            mtHtml = extMainText(unicode(html, "utf-8"), threshold, debug)
            # Transfer to plain text:
            parser = html2txt()
            parser.reset()
            parser.feed(mtHtml)
            parser.close()
            text = parser.output()
            print text
        else:
            print "Can not open target html document!"
    