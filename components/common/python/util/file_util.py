import os
from os.path import dirname

class FileUtil(object):
    '''Utility functions to manage files and directories.'''
    
    @staticmethod
    def make_dir(path):
        '''Create directories recursively.'''
        os.makedirs(path, exist_ok=True)
        
    @staticmethod
    def make_file_dir(path):
        '''Create a file directories recursively.'''
        file_dir = dirname(path)
        if file_dir:
            FileUtil.make_dir(file_dir)
