from _collections import OrderedDict

from ...model.execution.execution_info import ExecutionInfo
from .execution_message import ExecutionMessage
from ..job.parent_job import ParentJob
from ...rest.foreign_attribute import ForeignAttribute
from ...rest.foreign_key import ForeignKey


class ExecutionInfoToMessage(ForeignKey):
    '''
    Association between one ExecutionInfo and one ExecutionMessage entries.

    :param time: Message emission date
    :param log_level: Either logging.CRITICAL, ERROR, WARNING, INFO, or DEBUG
    :param execution_info: ExecutionInfo instance
    :param execution_message: ExecutionMessage instance
    :param parent_job: ParentJob instance
    '''

    # Database table name
    TABLE_NAME = "execution_info_to_messages"

    # Foreign key attribute to execution_info
    # In PostgreSQL: fk_execution_info_id int references cosims.execution_info(id) on delete cascade
    # In Python: the ExecutionInfo class corresponds to the 'execution_info' attribute and
    # to the cosims.execution_info database table.
    FOREIGN_INFO = ForeignAttribute(
        foreign_id='fk_execution_info_id',
        foreign_table=ExecutionInfo.TABLE_NAME,
        _type=ExecutionInfo,
        name='execution_info')

    # Foreign key attribute to execution_message
    # In PostgreSQL: fk_execution_message_id int references cosims.execution_message(id) on delete cascade
    # In Python: the ExecutionMessage class corresponds to the 'execution_message' attribute and
    # to the cosims.execution_message database table.
    FOREIGN_MESSAGE = ForeignAttribute(
        foreign_id='fk_execution_message_id',
        foreign_table=ExecutionMessage.TABLE_NAME,
        _type=ExecutionMessage,
        name='execution_message')

    # Foreign key attribute to job.
    # In PostgreSQL: fk_parent_job_id int references cosims.parent_jobs(id) on delete cascade
    # In Python: the ParentJob class corresponds to the 'parent_job' attribute and
    # to the cosims.parent_jobs database table.
    # It is the "parent" table of the other job tables.
    FOREIGN_PARENT_JOB = ForeignAttribute(
        foreign_id='fk_parent_job_id',
        foreign_table=ParentJob.TABLE_NAME,
        _type=ParentJob,
        name='parent_job')

    def __init__(self, time=None, log_level=None, execution_info=None, execution_message=None):

        self.time = time
        self.log_level = log_level

        # Duplicate execution_info.job object so it can be queried by PostgREST
        parent_job = None
        if execution_info:
            parent_job = execution_info.parent_job

        # Call the parent constructor AFTER all the attributes are initialized
        super().__init__(

            # Table name for this class
            ExecutionInfoToMessage.TABLE_NAME,

            # Foreign key attributes with their existing values.
            # parent_job must be first so it is accessed with higher priority than execution_info.parent_job
            OrderedDict({
                ExecutionInfoToMessage.FOREIGN_PARENT_JOB: parent_job,
                ExecutionInfoToMessage.FOREIGN_INFO: execution_info,
                ExecutionInfoToMessage.FOREIGN_MESSAGE: execution_message,
            }))

    #
    # Build a string for PostgREST query parameters

    def message_like(self, value, case_sensitive=False):
        '''execution_messages.body=like.value'''
        return super().attribute_like(
            '%s.body' %
            ExecutionInfoToMessage.FOREIGN_MESSAGE.foreign_table, value, case_sensitive)
