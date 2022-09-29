from .child_job import ChildJob
from .job_status import JobStatus
from .parent_job import ParentJob
from ...rest.foreign_attribute import ForeignAttribute
from ...rest.foreign_key import ForeignKey
from ....util.log_util import temp_logger


class JobStatusChange(ForeignKey):
    '''
    One entry (=status change) in the job and job status history, with the status change date.
    
    :param time: status change date
    :param job_status: JobStatus value  
    :param parent_job: ParentJob instance
    '''

    # Database table name
    TABLE_NAME = "job_status_changes"

    # Counter to log only one error message
    __DATABASE_PREVIOUSLY_IN_ERROR = False

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
    
    def __init__(self, child_job, time, job_status, **kwds):
        
        self.time = time
        self.error_subtype = None
        self.error_message = None
        self.job_status = job_status
        
        # The child_job parameter must inherit the ChildJob class
        if not isinstance(child_job, ChildJob):
            raise Exception('Wrong job type \'%s\', must be \'ChildJob\'.' % type(child_job).__name__)

        # ParentJob object corresponding to the job database table
        # (the ParentJob instance is the foreign attribute from the ChildJob instance)
        parent_job = child_job.parent_job

        if (not parent_job) or (not child_job.id) or (not parent_job.id):
            if not JobStatusChange.__DATABASE_PREVIOUSLY_IN_ERROR:
                temp_logger.error("ParentJob database IDs must be set (the entries must exist in the database).")
                JobStatusChange.__DATABASE_PREVIOUSLY_IN_ERROR = True
        else:
                JobStatusChange.__DATABASE_PREVIOUSLY_IN_ERROR = False

        # Call the parent constructor AFTER all the attributes are initialized
        super().__init__(

            # Table name for this class
            JobStatusChange.TABLE_NAME,

            # Foreign key attributes with their existing values.
            {JobStatusChange.FOREIGN_PARENT_JOB: parent_job})

        # Attribute values given by the caller
        for key, value in kwds.items():
            setattr(self, key, value)

    def to_database_value(self, attribute, value):
        '''Return a value as it must be inserted in the database.'''

        # Return JobStatus.value which is the same as in the SQL table job_status
        if isinstance(value, JobStatus):
            return value.value

        # Default: call parent class
        return super().to_database_value(attribute, value)

    def from_database_value(self, attribute, value):
        '''
        Parse a string value as it was inserted in the database.
        To be implemented by child classes.
        '''

        # Return JobStatus from its int value, which corresponds 
        # to the SQL table job_status entry.
        if isinstance(value, JobStatus):
            return JobStatus(value)

        # Default: call parent class
        return super().from_database_value(attribute, value)
        
        