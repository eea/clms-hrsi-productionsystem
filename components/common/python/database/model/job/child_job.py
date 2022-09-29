'''
_summary_

:raises NotImplementedError: _description_
:return: _description_
:rtype: _type_
'''

from .parent_job import ParentJob
from .job_status import JobStatus
from ...rest.foreign_attribute import ForeignAttribute
from ...rest.foreign_key import ForeignKey
from ....util.log_util import temp_logger


class ChildJob(ForeignKey):
    '''
    Python interface for the child job classes and SQL tables (FSC/RLIE jobs, 
    PSA jobs, Arlie jobs, other jobs).

    :param ForeignKey: _description_
    :type ForeignKey: _type_
    :raises NotImplementedError: _description_
    :return: _description_
    :rtype: _type_
    '''

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
    logger = None

    def __init__(self, table_name=''):

        # Call the parent constructor AFTER all the attributes are initialized
        super().__init__(# Table name for the child class
                         table_name,
                         # Foreign key attributes with no initial value
                         {ChildJob.FOREIGN_PARENT_JOB: None})


    def post_new_execution_info(self):
        '''
        Create, save and return new execution info about this job.
        This job must already be saved in the database.
        '''

        # Import module here to avoid mutual inclusion
        from ..execution.execution_info import ExecutionInfo

        # Create execution info attached to the job.
        # post_foreign=False because the foreign attribute (this job) must already exist in the database.
        execution_info = ExecutionInfo(self)
        execution_info.post(post_foreign=False, logger_func=temp_logger.debug)
        return execution_info

    def post_new_status_change(self, job_status, error_subtype=None, error_message=None, logger_func=temp_logger.debug):
        '''Create, save and return a new job status change into the database.'''

        # Import module here to avoid mutual inclusion
        from .job_status_change import JobStatusChange

        # Create a new instance with time=now
        job_status_change = JobStatusChange(
            child_job=self,
            time=None,
            error_subtype=error_subtype,
            error_message=error_message,
            job_status=job_status)

        # Save the entry into the database.
        # # post_foreign=False because the foreign attribute (this job)
        # must already exist in the database.
        job_status_change.post(post_foreign=False, logger_func=logger_func)
        return job_status_change

    def start(self, *args):
        '''Perform pre- and post-processing and start the job execution.'''

        # Create and save new execution info about this job.
        execution_info = self.post_new_execution_info()

        # Import module here to avoid mutual inclusion
        from ...logger import Logger

        # Create a custom logger instance to save messages into the database.
        self.logger = Logger(execution_info)

        # Start child execution
        self.wrapped_start(*args)

        # Update the job status
        self.post_new_status_change(JobStatus.processed)

    def wrapped_start(self, *args):
        '''
        Start job execution, wrapped by pre- and post-processing.
        Must be implemented by the child classes.

        :raises NotImplementedError: Unimplemented function
        '''
        raise NotImplementedError('Unimplemented function')

    #
    # Build a string for PostgREST query parameters

    def id_eq(self, value):
        '''
        id=eq.value
        '''
        return super().attribute_eq('id', value)

    def name_eq(self, value):
        '''
        parent_jobs.name=eq.value
        '''
        return super().attribute_eq(f'{ChildJob.FOREIGN_PARENT_JOB.foreign_table}.name', value)

    def tile_id_eq(self, value):
        '''
        jobs.tile_id=eq.value
        '''
        return super().attribute_eq(f'{ChildJob.FOREIGN_JOB.foreign_table}.tile_id',value)
