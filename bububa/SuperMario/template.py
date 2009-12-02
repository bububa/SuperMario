#!/usr/bin/env python
# encoding: utf-8
"""
template.py
SuperMario

Created by Syd on 2009-08-05.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""
import re
from templatemaker import Template

PLACE_HOLDER = '{PLACE_HOLDER}'
class BaseTemplate:
    
    def __init__(self, tolerance):
        self.template = Template(tolerance=tolerance)
    
    def learn(self, inputs):
        if not inputs: return None
        map(self.template.learn, inputs)
    
    def extract(self, sample):
        return self.template.extract(sample)
    
    def pattern(self, urls):
        tmp = self.template.as_text(PLACE_HOLDER)
        cols = {}
        for url in urls:
            cols = self.checkFieldType(url, cols)
        for col in xrange(0, len(cols)):
            tmp = re.sub(PLACE_HOLDER, cols[col], tmp)
        return tmp
    
    def checkFieldType(self, url, cols):
        col = 0
        fields = self.extract(url)
        for field in fields:
            if col not in cols:
                cols[col] = '(\d+)'
            try:
                isInt = int(field)
            except:
                cols[col] = '(.+)'
        return cols