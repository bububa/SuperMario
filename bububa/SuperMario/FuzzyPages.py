#!/usr/bin/env python
# encoding: utf-8
"""
FuzzyPages.py

Created by Syd on 2009-08-06.
Copyright (c) 2009 __MyCompanyName__. All rights reserved.
"""
# 文件减肥；版本0.2；作者songjl@163.net
# 对某一目录下的文件进行检索，合并其中比较相似的文件，对于html文件，同时对文件中的链接进行相应修改
# 具体使用范例见本文件最后“if __name__=="__main__":”部分的操作
# @History:
# 0.1, 用函数的形式实现了对目录下文件的相似度进行比较的功能
# 0.2, 改写为类的形式，基本实现所有功能
# 0.3, 不再使用dptools库，改为使用difflib实现文件比较（其中的unified_diff至少与dptools一样快，结果还更准确）
# 0.4, 补充了对html中使用的css定义的替换，由使用head中的css定义改为使用外部css，减小文件大小

import os,re,difflib

class FuzzyPages:
    whiteList=None	# 不予合并的文件白名单
    silenceMode=None	# 安静模式，不给出任何提示信息的运行方式
    maxSizeDiff=None	# 如果文件之间的差别大于此大小（单位：字节），那么直接认为文件差别很大
    maxDiffLine=None	# 如果两文件之间差别的行数小于等于此值，则认为文件可以合并
    baseDir=None	# 类所处理的路径
    
    """ 初始化函数，必须给出所要处理的路径
    """
    def __init__(self,baseDir,whiteList=r'\n\t',maxSizeDiff=1000,maxDiffLine=1,silenceMode=True):
	self.baseDir=baseDir
	self.maxSizeDiff=maxSizeDiff
	self.maxDiffLine=maxDiffLine
	self.silenceMode=silenceMode
	self.whiteList=whiteList
	
    """ 列出给定目录下的所有文件，包括对子目录的展开
    """
    def lsPlus(self,dir=None):
	if (dir==None):
	    dir=self.baseDir
	allFiles=[]
        aFile=None
        for file in os.listdir(dir):
            aFile=dir+'\\'+file
            if (os.path.isdir(aFile)):
                allFiles.extend(self.lsPlus(aFile))
            else:
                allFiles.append(aFile)
        return allFiles
	
    """ 过滤给出的参数列表，仅保留以html结尾的文件
    """
    def getHtmlFiles(self,fileList):
	return filter(lambda f:f.endswith('.html') or f.endswith('.htm'),fileList)
	
    """ 读取列表中每个文件的内容，存入文件内容列表。内容列表的存放顺序与输入的文件列表一致
        对每个html文件，仅考虑其中“<head”标签和“/body>”标签之间的部分。
    """
    def getFilesContent(self,fileList):
        contentList=[]
        sizeList=[]
        bodyRe=re.compile(r'<head.*/body>',re.I | re.S)
        for file in fileList:
            tmpFile=open(file,'r')
            str=tmpFile.read()
            tmpFile.close()
            match=bodyRe.search(str)
            if (match==None):
                contentList.append(str.splitlines(True))
            else:
                contentList.append(match.group().splitlines(True))
            sizeList.append(os.path.getsize(file))
        return (contentList,sizeList)
	
    """ 比较输入的两个字符串的差异程度
    """
    def getWeight(self,strList1,strList2):
        diff=list(difflib.unified_diff(strList1,strList2))
	plusCount=0
	minusCount=0
	for d in diff[3:]:
	    dChar=d[0]
	    if (dChar=='+'):
		plusCount+=1
	    elif (dChar=='-'):
		minusCount+=1
        if (plusCount>=minusCount):
            return plusCount
        else:
            return minusCount
	    
    """ 以笛卡儿积的形式比较输入列表中的任意两个元素的权值，并输出这个权值的分布情况
    """
    def getWeightRange(self,fileInfoTurple):
        contentList=fileInfoTurple[0]
        sizeList=fileInfoTurple[1]
        weightRange={}
        calcedNum=0
        listLen=len(contentList)
        for i in range(listLen):
            if (not self.silenceMode):
		print 'now in',i
            list1=contentList[i]
            for j in range(i+1,listLen):
                if (abs(sizeList[i]-sizeList[j])<self.maxSizeDiff):
                    calcedNum+=1
                    list2=contentList[j]
                    w=self.getWeight(list1,list2)
                    weightCount=weightRange.get(w,0)
                    weightCount+=1
                    weightRange[w]=weightCount
        if (not self.silenceMode):
	    for weight in weightRange.keys():
                print weight,weightRange[weight]
            print 'Compared files num:',calcedNum
        return

    """ 两两比较fileList给出的文件，生成其中比较相似的文件的替换表
    """
    def getFuzzyTable(self,fileList):
	if (not self.silenceMode):
	    print self.whiteList
	whiteListRe=re.compile(self.whiteList,re.I)
	contentList,sizeList=self.getFilesContent(fileList)
        fuzzyTable={}
	compareList=[]
	for i in range(len(fileList)):
	    if (len(compareList)==0 or whiteListRe.search(fileList[i])<>None):
		if (not self.silenceMode):
		    print 'Jumped:',fileList[i]
		fuzzyTable[fileList[i]]=""
		compareList.append(i)
		continue
	    list1=contentList[i]
	    for j in compareList:
		if (abs(sizeList[i]-sizeList[j])<self.maxSizeDiff):
		    list2=contentList[j]
		    if (self.getWeight(list1,list2)<=self.maxDiffLine):
			fuzzyTable[fileList[i]]=fileList[j]
			break
	    else:
		fuzzyTable[fileList[i]]=""
		compareList.append(i)
	return fuzzyTable
	
    """ 对相似文件进行合并处理，用到了相似文件替换表
        目前仅处理了对同一目录下文件之间文件的替换
        文件中每行只能有一个.html文件
    """
    def doFuzzy(self,fileList):
	fuzzyTable=self.getFuzzyTable(fileList)
	if (not self.silenceMode):
	    print 'Fuzzy table ok!'
	    #return
	for file in fileList:
	    if (not self.silenceMode):
		print 'Now in:',file
	    if (fuzzyTable[file]==''):
		fFile=open(file,'r')
		str=fFile.read()
		fFile.close()
		lineList=str.splitlines(True)
		newList=[]
		hrefRe=re.compile(r'([="])(?:([\w\-_~]*?\.html)|([\w\-_~]*?\.htm))',re.I)
		for str in lineList:
		    matchList=hrefRe.split(str)
		    str=''
		    for piece in matchList:
		        if (piece==None):
			    piece=''
		        elif (piece.find('.html')<>-1 or piece.find('.htm')<>-1):
			    newLink=fuzzyTable.get(self.baseDir+'\\'+piece)
			    if (newLink==None or newLink==''):
				newLink=self.baseDir+'\\'+piece
				if (not self.silenceMode):
				    pass#print 'Fuzzy Key Error:',file,newLink
			    piece=os.path.basename(newLink)
		        str+=piece
		    newList.append(str)
		str=''.join(newList)
		fFile=open(file,'w')
		fFile.write(str)
		fFile.close()
	    else:
		os.remove(file)
	return True

    """ 切换文件使用外部CSS而不是页面内部的CSS定义
    """
    def changeCssUse(self,fileList):
        for file in fileList:
            fileReader=open(file,'r')
            content=fileReader.read()
            fileReader.close()
            content=content.replace('<!-- link rel="stylesheet" href="templates/subSilver/subSilver.css" type="text/css" -->','<link rel="stylesheet" href="templates/subSilver/subSilver.css" type="text/css" />')
            content=content.replace('''<style type="text/css">
<!--
/*
  The original subSilver Theme for phpBB version 2+
  Created by subBlue design
  http://www.subBlue.com

  NOTE: These CSS definitions are stored within the main page body so that you can use the phpBB2
  theme administration centre. When you have finalised your style you could cut the final CSS code
  and place it in an external file, deleting this section to save bandwidth.
*/

/* General page style. The scroll bar colours only visible in IE5.5+ */
body {
	background-color: #E5E5E5;
	scrollbar-face-color: #DEE3E7;
	scrollbar-highlight-color: #FFFFFF;
	scrollbar-shadow-color: #DEE3E7;
	scrollbar-3dlight-color: #D1D7DC;
	scrollbar-arrow-color:  #006699;
	scrollbar-track-color: #EFEFEF;
	scrollbar-darkshadow-color: #98AAB1;
}

/* General font families for common tags */
font,th,td,p { font-family: Verdana, Arial, Helvetica, sans-serif }
a:link,a:active,a:visited { color : #006699; }
a:hover		{ text-decoration: underline; color : #DD6900; }
hr	{ height: 0px; border: solid #D1D7DC 0px; border-top-width: 1px;}

/* This is the border line & background colour round the entire page */
.bodyline	{ background-color: #FFFFFF; border: 1px #98AAB1 solid; }

/* This is the outline round the main forum tables */
.forumline	{ background-color: #FFFFFF; border: 2px #006699 solid; }

/* Main table cell colours and backgrounds */
td.row1	{ background-color: #EFEFEF; }
td.row2	{ background-color: #DEE3E7; }
td.row3	{ background-color: #D1D7DC; }

/*
  This is for the table cell above the Topics, Post & Last posts on the index.php page
  By default this is the fading out gradiated silver background.
  However, you could replace this with a bitmap specific for each forum
*/
td.rowpic {
		background-color: #FFFFFF;
		background-image: url(templates/subSilver/images/cellpic2.jpg);
		background-repeat: repeat-y;
}

/* Header cells - the blue and silver gradient backgrounds */
th	{
	color: #FFA34F; font-size: 12px; font-weight : bold;
	background-color: #006699; height: 25px;
	background-image: url(templates/subSilver/images/cellpic3.gif);
}

td.cat,td.catHead,td.catSides,td.catLeft,td.catRight,td.catBottom {
			background-image: url(templates/subSilver/images/cellpic1.gif);
			background-color:#D1D7DC; border: #FFFFFF; border-style: solid; height: 28px;
}

/*
  Setting additional nice inner borders for the main table cells.
  The names indicate which sides the border will be on.
  Don't worry if you don't understand this, just ignore it :-)
*/
td.cat,td.catHead,td.catBottom {
	height: 29px;
	border-width: 0px 0px 0px 0px;
}
th.thHead,th.thSides,th.thTop,th.thLeft,th.thRight,th.thBottom,th.thCornerL,th.thCornerR {
	font-weight: bold; border: #FFFFFF; border-style: solid; height: 28px;
}
td.row3Right,td.spaceRow {
	background-color: #D1D7DC; border: #FFFFFF; border-style: solid;
}

th.thHead,td.catHead { font-size: 12px; border-width: 1px 1px 0px 1px; }
th.thSides,td.catSides,td.spaceRow	 { border-width: 0px 1px 0px 1px; }
th.thRight,td.catRight,td.row3Right	 { border-width: 0px 1px 0px 0px; }
th.thLeft,td.catLeft	  { border-width: 0px 0px 0px 1px; }
th.thBottom,td.catBottom  { border-width: 0px 1px 1px 1px; }
th.thTop	 { border-width: 1px 0px 0px 0px; }
th.thCornerL { border-width: 1px 0px 0px 1px; }
th.thCornerR { border-width: 1px 1px 0px 0px; }

/* The largest text used in the index page title and toptic title etc. */
.maintitle	{
	font-weight: bold; font-size: 22px; font-family: "Trebuchet MS",Verdana, Arial, Helvetica, sans-serif;
	text-decoration: none; line-height : 120%; color : #000000;
}

/* General text */
.gen { font-size : 12px; }
.genmed { font-size : 12px; }
.gensmall { font-size : 12px; }
.gen,.genmed,.gensmall { color : #000000; }
a.gen,a.genmed,a.gensmall { color: #006699; text-decoration: none; }
a.gen:hover,a.genmed:hover,a.gensmall:hover	{ color: #DD6900; text-decoration: underline; }

/* The register, login, search etc links at the top of the page */
.mainmenu		{ font-size : 12px; color : #000000 }
a.mainmenu		{ text-decoration: none; color : #006699;  }
a.mainmenu:hover{ text-decoration: underline; color : #DD6900; }

/* Forum category titles */
.cattitle		{ font-weight: bold; font-size: 12px ; letter-spacing: 1px; color : #006699}
a.cattitle		{ text-decoration: none; color : #006699; }
a.cattitle:hover{ text-decoration: underline; }

/* Forum title: Text and link to the forums used in: index.php */
.forumlink		{ font-weight: bold; font-size: 12px; color : #006699; }
a.forumlink 	{ text-decoration: none; color : #006699; }
a.forumlink:hover{ text-decoration: underline; color : #DD6900; }

/* Used for the navigation text, (Page 1,2,3 etc) and the navigation bar when in a forum */
.nav			{ font-weight: bold; font-size: 12px; color : #000000;}
a.nav			{ text-decoration: none; color : #006699; }
a.nav:hover		{ text-decoration: underline; }

/* titles for the topics: could specify viewed link colour too */
.topictitle,h1,h2	{ font-weight: bold; font-size: 12px; color : #000000; }
a.topictitle:link   { text-decoration: none; color : #006699; }
a.topictitle:visited { text-decoration: none; color : #5493B4; }
a.topictitle:hover	{ text-decoration: underline; color : #DD6900; }

/* Name of poster in viewmsg.php and viewtopic.php and other places */
.name			{ font-size : 12px; color : #000000;}

/* Location, number of posts, post date etc */
.postdetails		{ font-size : 12px; color : #000000; }

/* The content of the posts (body of text) */
.postbody { font-size : 12px; line-height: 18px}
a.postlink:link	{ text-decoration: none; color : #006699 }
a.postlink:visited { text-decoration: none; color : #5493B4; }
a.postlink:hover { text-decoration: underline; color : #DD6900}

/* Quote & Code blocks */
.code {
	font-family: Courier, 'Courier New', sans-serif; font-size: 12px; color: #006600;
	background-color: #FAFAFA; border: #D1D7DC; border-style: solid;
	border-left-width: 1px; border-top-width: 1px; border-right-width: 1px; border-bottom-width: 1px
}

.quote {
	font-family: Verdana, Arial, Helvetica, sans-serif; font-size: 12px; color: #444444; line-height: 125%;
	background-color: #FAFAFA; border: #D1D7DC; border-style: solid;
	border-left-width: 1px; border-top-width: 1px; border-right-width: 1px; border-bottom-width: 1px
}

/* Copyright and bottom info */
.copyright		{ font-size: 12px; font-family: Verdana, Arial, Helvetica, sans-serif; color: #444444; letter-spacing: -1px;}
a.copyright		{ color: #444444; text-decoration: none;}
a.copyright:hover { color: #000000; text-decoration: underline;}

/* Form elements */
input,textarea, select {
	color : #000000;
	font: normal 12px Verdana, Arial, Helvetica, sans-serif;
	border-color : #000000;
}

/* The text input fields background colour */
input.post, textarea.post, select {
	background-color : #FFFFFF;
}

input { text-indent : 2px; }

/* The buttons used for bbCode styling in message post */
input.button {
	background-color : #EFEFEF;
	color : #000000;
	font-size: 12px; font-family: Verdana, Arial, Helvetica, sans-serif;
}

/* The main submit button option */
input.mainoption {
	background-color : #FAFAFA;
	font-weight : bold;
}

/* None-bold submit button */
input.liteoption {
	background-color : #FAFAFA;
	font-weight : normal;
}

/* This is the line in the posting page which shows the rollover
  help line. This is actually a text box, but if set to be the same
  colour as the background no one will know ;)
*/
.helpline { background-color: #DEE3E7; border-style: none; }

/* Import the fancy styles for IE only (NS4.x doesn't use the @import function) */
@import url("templates/subSilver/formIE.css");
-->
</style>''','')
            fileWriter=open(file,'w')
            fileWriter.write(content)
            fileWriter.close()
        print 'CSS has been changed'

if __name__=="__main__":
    """ 这里的例子是使用本文件提供的函数，处理httrack程序下载的phpbb2论坛页面，对下载页面
        中的冗余部分进行去除和合并，以达到减小文件大小，以供存储的目的。
        原理是：如果两个html页面十分相似（用diff检查的差别小于定义的最大差别），那么删除其中
        的一个，将其余文件中指向被删除文件的链接改为指向与之相似的文件。
        具体步骤见下。
    """
    """ 定义我存放httrack下载的bbs静态页面的存储路径：
    """
    dir='E:\\WorkNow\\Source\\FuzzyPages\\bbs'
    """ 初始化一个FuzzyPages对象
        使用上面定义的bbs存储路径dir
        使用白名单：正则表达式r'view\w*?\.html|prof\w*?\.html'
            意思是对以view和prof开头的html文件不进行删除操作，因为这些文件保存了论坛中的
            帖子和用户帐号信息，不能被合并。而且如果不把这些文件列入白名单，那么index文件
            指向的链接很多都会被替换，造成一些论坛页面无法访问到，所以不得不启用白名单机制。
        如果diff比较的结果的差别行数不大于2，那么就对两个文件进行合并。因为有白名单机制，所
            以把maxDiffLine设置为2，而不是默认的1。
        设定silenceMode为False，因为我们需要Python打印一些信息出来看看。不过如果使用
            silenceMode，那么程序运行的速度大约能提高一倍，看来Python的打印函数很重啊。
    """
    fp=FuzzyPages(dir,whiteList=r'view\w*?\.html|prof\w*?\.html',maxDiffLine=2,silenceMode=False)
    """ 在开始处理文件之前，我们用下面的语句来看看文件的相似度分布情况，是不是有很多文件差别很小呢？
    """
    #fp.getWeightRange(fp.getFilesContent(fp.getHtmlFiles(fp.lsPlus())))
    """ getFuzzyTable能产生一个文件的替换表，我打印出此替换表的长度，看其是否正常工作了。
    """
    #print len(fp.getFuzzyTable(fp.getHtmlFiles(fp.lsPlus())))
    """ 进行替换过程。
    """
    if (fp.doFuzzy(fp.getHtmlFiles(fp.lsPlus()))):
	print 'Fuzzy pages ok!'
    else:
	print 'Fuzzy failed!'
    """ 后来发现phpbb2的文件把CSS定义写在每一页面的head标签里面了，占用了很大空间，把这些
        内容挪到外部CSS共用，能够有效减小文件大小。
    """
    #fp.changeCssUse(fp.getHtmlFiles(fp.lsPlus()))

