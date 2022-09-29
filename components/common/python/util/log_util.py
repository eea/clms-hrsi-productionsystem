import logging
import sys

from .file_util import FileUtil


class LogUtil(object):
    '''Utility functions to manage log messages.'''

    # Logging parameters
    FORMAT = '[%(levelname)s] [%(name)s] %(asctime)s - %(message)s'
    # FORMAT = '[%(name)s] [%(levelname)s] %(asctime)s.%(msecs)03d - %(message)s'
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    # DATE_FORMAT = '%H:%M:%S'
    
    @staticmethod
    def add_file_handler(logger, log_file_path):
        
        # Create formatter
        formatter = logging.Formatter(LogUtil.FORMAT)
        formatter.datefmt = LogUtil.DATE_FORMAT
        
        # Create the file directory
        FileUtil.make_file_dir(log_file_path)

        # Create file handler
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger

    @staticmethod
    def get_logger(log_name, log_level=None, log_file_path=None):
        '''
        Create and return a logger object.

        :param log_name: e.g. component name.
        :param log_level: either logging.CRITICAL, ERROR, WARNING, INFO, or DEBUG
        :param log_file_path: Log file path, optional.
        '''

        # Create logger with name
        logger = logging.getLogger(log_name)

        # Remove previously defined handlers, if any
        logger.handlers = []

        # Set minimum log level
        if log_level:
            logger.setLevel(log_level)

        # Create formatter
        formatter = logging.Formatter(LogUtil.FORMAT)
        formatter.datefmt = LogUtil.DATE_FORMAT

        # DEBUG and INFO go to stdout
        class InfoFilter(logging.Filter):
            def filter(self, rec):
                return rec.levelno in (logging.DEBUG, logging.INFO)
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)        
        stdout_handler.setLevel(logging.DEBUG)
        stdout_handler.addFilter(InfoFilter())
        logger.addHandler(stdout_handler)
        
        # WARNING and above go to stderr
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setFormatter(formatter)        
        stderr_handler.setLevel(logging.WARNING)
        logger.addHandler(stderr_handler)

        if log_file_path:
            LogUtil.add_file_handler(logger, log_file_path)
                
        return logger
        



# Temp logger used when no other logger is available
temp_logger = LogUtil.get_logger('temp', log_level=logging.INFO)
