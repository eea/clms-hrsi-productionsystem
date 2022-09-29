from datetime import datetime
import logging
from threading import Lock
import traceback

from ..database.model.execution.execution_info_to_message import ExecutionInfoToMessage
from ..database.model.execution.execution_message import ExecutionMessage
from ..util.log_util import LogUtil
from ..database.rest.stored_procedure import StoredProcedure


class Logger(object):
    '''
    Custom logger to save messages into the database, attached to a job execution.
    We override the __getattr__ function to simulate the fact that the 
    current class would be a child class of the native Python logger class.

    :param execution_info: Attached job execution information.
    '''

    def __init__(self, execution_info):
        '''
        Constructor.
        :param execution_info: Attached job execution info.
        '''

        # Save execution info
        self.execution_info = execution_info

        # Log name = job name
        log_name = execution_info.parent_job.name
        if log_name is None:
            log_name = ''

        # Build the native Python logger class
        self.native_logger = LogUtil.get_logger(
            log_name,
            log_level=self.execution_info.min_log_level,
            log_file_path=self.execution_info.log_file_path)

    def __getattr__(self, attr):
        '''
        Simulate the fact that the current class would be a child class of the foreign attributes.
        This function is called only when the attribute does not exist in the current object.
        '''
        return getattr(self.native_logger, attr)

    __lock_save_message = Lock()

    def save_message(self, log_level, msg, *args, exc_info=None):
        '''Save a log message into the database.'''

        # For now we completly disable the use of the DB to store log messages.
        # First tests show that this increases dramatically the size of the DB.
        #
        # TODO choose an alternative way to store log message and/or make
        # sure there are less messages.
        return

        # If the message log level is too low, do nothing
        if not self.isEnabledFor(log_level):
            return

        # Build the message body,
        # e.g. msg= 'My message : %s, %s'
        # args = ('foo', 'bar')
        # body = 'My message : foo, bar'
        body = str(msg)
        if args:
            body = body % args

        # No simultaneous calls
        with Logger.__lock_save_message:

            # Does this message body already exist in database ?
            # We do this search and insert inside a Lock section so multiple threads
            # will not insert duplicate messages at the same time. But it does not
            # work in case of multiple processes.
            #
            # TODO maybe we should insert messages and parameters in different tables
            # to avoid duplicates,
            # e.g. 'My message : foo' and 'My message : bar' would be inserted two times,
            # but 'My message : %s' could be inserted a single time, whith the 'foo' and
            # 'bar' parameters inserted into a different table.

            # Log REST queries.
            # Use self.native_logger to avoid recursive calls.
            # logger_func = self.native_logger.debug # too verbose. I
            logger_func = None

            # Find all messages from the database with this body.
            # We could call the GET method: ?body=eq.'my body' but it could fail
            # because the GET urL is too long when the body is too long.
            messages = StoredProcedure.messages_with_body(body, logger_func)

            # If at least one exists, keep the first one, ignore others (duplicates)
            if messages:
                message = messages[0]

            # Else create a new message with this body and insert it into the database
            else:
                message = ExecutionMessage(body)
                message.post(logger_func=logger_func)

            # Association between the execution info and message.
            # Emission date = now
            # post_foreign=False because the foreign attributes already exist in the database.
            ExecutionInfoToMessage(
                time=datetime.utcnow(),
                log_level=log_level,
                execution_info=self.execution_info,
                execution_message=message
            ).post(
                post_foreign=False,
                logger_func=logger_func)

        # Also save the traceback
        if exc_info:
            self.save_message(log_level, msg=traceback.format_exc(), exc_info=None)

    #
    # Override the native logger functions: call the native function
    #  then save message into the database.

    def debug(self, msg, *args, **kwargs):
        self.native_logger.debug(msg, *args, **kwargs)
        self.save_message(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.native_logger.info(msg, *args, **kwargs)
        self.save_message(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.native_logger.warning(msg, *args, **kwargs)
        self.save_message(logging.WARNING, msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        self.native_logger.warn(msg, *args, **kwargs)
        self.save_message(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.native_logger.error(msg, *args, **kwargs)
        self.save_message(logging.ERROR, msg, *args, **kwargs)

    def exception(self, msg, *args, exc_info=True, **kwargs):
        self.native_logger.exception(msg, *args, exc_info=exc_info, **kwargs)
        self.save_message(logging.ERROR, msg, *args, exc_info=exc_info, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.native_logger.critical(msg, *args, **kwargs)
        self.save_message(logging.CRITICAL, msg, *args, **kwargs)
