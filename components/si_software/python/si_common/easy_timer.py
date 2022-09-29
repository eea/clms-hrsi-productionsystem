#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#######################################################################
#  This code has been developped by Magellium SAS
#
#  Licensing:
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>
#######################################################################

import os, sys, shutil
import time



class Timer(object):
    
    def __init__(self, from_str='From beginning'):
        self.times = [time.time()]
        self.from_ref_str = [from_str]
        self.last_time = None
        
    def reset(self):
        self.times = [time.time()]
        self.from_ref_str = self.from_ref_str[0:1]
        self.last_time = None
        
    def check_levels(self):
        if len(self.times) != len(self.from_ref_str):
            raise Exception('level mismatch')
            
    def level_up(self, from_str):
        self.check_levels()
        self.times.append(time.time())
        self.from_ref_str.append(from_str)
        
    def level_down(self):
        self.check_levels()
        if len(self.times) == 1:
            raise Exception('cannot go under size 1 list')
        self.times = self.times[0:-1]
        self.from_ref_str = self.from_ref_str[0:-1]
        self.last_time = None
        
    def get_time_str(self, nice=False):
        self.check_levels()
        tnow = time.time()
        str_out = ''
        separator = ''
        for ii in range(len(self.times)):
            str_out += separator + '%s: %s seconds'%(self.from_ref_str[ii], tnow-self.times[ii])
            if nice:
                separator = '\n' + ' '*((ii+1)*2)
            else:
                separator = '; '
        if self.last_time is not None:
            str_out += separator + 'From last time measurement: %s seconds'%(tnow-self.last_time)
        self.last_time = tnow
        return str_out
        
    def print_time(self, msg=None, nice=False, str_gap=0):
        if msg is None:
            print(' '*str_gap + self.get_time_str(nice=nice).replace('\n','\n' + ' '*str_gap))
        else:
            if nice:
                print(' '*str_gap + msg + ':\n  ' + ' '*str_gap + self.get_time_str(nice=nice).replace('\n','\n  ' + ' '*str_gap))
            else:
                print(' '*str_gap + msg + ': ' + self.get_time_str(nice=nice))
        
    def __str__(self):
        return self.get_time_str()
        
        
        
        
