import os
import sys
from .log_util import temp_logger


class SysUtil(object):
    '''System-related utility functions.'''
    
    @staticmethod
    def read_env_var(name):
        '''Return an environment variable value. Throw exception if doesn't exist.'''
        try:
            return os.environ[name]
        except KeyError:
            raise Exception ('Environment variable is missing: %s' % name)

    @staticmethod
    def ensure_env_var_set(name):
        '''Ensure that an environment variable is set. Exit in error if it doesn't exist.'''
        if name not in os.environ:
            temp_logger.error("'%s' environment variable not found!" % name)
            sys.exit(1)