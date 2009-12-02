#!/usr/bin/env python
# encoding: utf-8
"""
CoreImage.py

Created by Syd on 2009-09-11.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""

import sys
import os
import Image, ImageEnhance

THUMB_SIZE=64

class CoreImage:
    
    def __init__(self):
        pass
    
    @staticmethod
    def open(filename):
        return Image.open(filename)
    
    @staticmethod
    def thumbnail(input_filename, output_filename, file_type='PNG', resize=THUMB_SIZE, mark=None, quality=90, compress=Image.ANTIALIAS):
        im = CoreImage.open(input_filename)
        im = CoreImage.resize_by_pixel(im, resize)
        if mark:
            im = CoreImage.watermark(im, mark, 'bottom-left')
        im.save(output_filename, file_type, quality=quality)
        
    @staticmethod
    def resize_by_percent(im, percent=1):
        w,h = im.size
        return im.resize(((percent*w)/100,(percent*h)/100))
    
    @staticmethod
    def resize_by_pixel(im, pixels=THUMB_SIZE):
        (wx,wy) = im.size
        rx=1.0*wx/pixels
        ry=1.0*wy/pixels
        if rx>ry:
            rr=rx
        else:
            rr=ry
        return im.resize((int(wx/rr), int(wy/rr)))
    
    @staticmethod
    def reduce_opacity(im, opacity):
        """Returns an image with reduced opacity."""
        assert opacity >= 0 and opacity <= 1
        if im.mode != 'RGBA':
            im = im.convert('RGBA')
        else:
            im = im.copy()
        alpha = im.split()[3]
        alpha = ImageEnhance.Brightness(alpha).enhance(opacity)
        im.putalpha(alpha)
        return im
    
    @staticmethod
    def watermark(im, mark, position, opacity=1):
        """Adds a watermark to an image."""
        if opacity < 1:
            mark = reduce_opacity(mark, opacity)
        if im.mode != 'RGBA':
            im = im.convert('RGBA')
        # create a transparent layer the size of the image and draw the
        # watermark in that layer.
        layer = Image.new('RGBA', im.size, (0,0,0,0))
        if position == 'tile':
            for y in range(0, im.size[1], mark.size[1]):
                for x in range(0, im.size[0], mark.size[0]):
                    layer.paste(mark, (x, y))
        elif position == 'scale':
            # scale, but preserve the aspect ratio
            ratio = min( float(im.size[0]) / mark.size[0], float(im.size[1]) / mark.size[1] )
            w = int(mark.size[0] * ratio)
            h = int(mark.size[1] * ratio)
            mark = mark.resize((w, h))
            layer.paste(mark, ((im.size[0] - w) / 2, (im.size[1] - h) / 2))
        elif position == 'bottom-left':
            layer.paste(mark, (5, im.size[1] - mark.size[1] - 5))
        elif position == 'bottom-right':
            layer.paste(mark, (im.size[0] - mark.size[0] - 5, im.size[1] - mark.size[1] - 5))
        elif position == 'top-left':
            layer.paste(mark, (5, 5))
        elif position == 'top-right':
            layer.paste(mark, (im.size[0] - mark.size[0] - 5, 5))
        else:
            layer.paste(mark, position)
        # composite the watermark with the layer
        return Image.composite(layer, im, layer)