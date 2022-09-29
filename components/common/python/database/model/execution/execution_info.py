
from ...model.job.child_job import ChildJob
from ...model.job.parent_job import ParentJob
from ...rest.foreign_attribute import ForeignAttribute
from ...rest.foreign_key import ForeignKey
from ....util.log_util import temp_logger


class ExecutionInfo(ForeignKey):
    '''
    Information about a job execution. 
    1 to many relationship between 1 job and many executions: 
    each job can be executed several times.

    :param min_log_level: Minimum level for log messages. 
    Either logging.CRITICAL, ERROR, WARNING, INFO, or DEBUG
    :param log_file_path: (str)
    '''

    # Database table name
    TABLE_NAME = "execution_info"

    # Counter to log only one error message
    __DATABASE_PREVIOUSLY_IN_ERROR = False

    # Foreign key attribute to job.
    # In PostgreSQL: fk_parent_job_id int references cosims.parent_jobs(id) on delete cascade
    # In Python: the ParentJob class corresponds to the 'parent_job' attribute and
    # to the cosims.parent_jobs database table.
    # It is the "parent" table of the other job tables.
    FOREIGN_PARENT_JOB = ForeignAttribute(
        foreign_id='fk_parent_job_id',
        foreign_table=ParentJob.TABLE_NAME,
        _type=ParentJob,
        name='parent_job')

    def __init__(self, child_job=None):

        if not child_job:
            parent_job = None

        else:
            # The child_job parameter must inherit the ChildJob class
            if not isinstance(child_job, ChildJob):
                raise Exception('Wrong job type \'%s\', must be \'ChildJob\'.' % type(child_job).__name__)

            # ParentJob object corresponding to the job database table
            # (the ParentJob instance is the foreign attribute from the ChildJob instance)
            parent_job = child_job.parent_job

            if (not parent_job) or (not child_job.id) or (not parent_job.id):
                if not ExecutionInfo.__DATABASE_PREVIOUSLY_IN_ERROR:
                    temp_logger.error("ParentJob database IDs must be set (the entries must exist in the database).")
                    ExecutionInfo.__DATABASE_PREVIOUSLY_IN_ERROR = True
            else:
                ExecutionInfo.__DATABASE_PREVIOUSLY_IN_ERROR = False

            # Log level and file path are those defined for the next job execution = this execution
            self.min_log_level = parent_job.next_log_level
            self.log_file_path = parent_job.next_log_file_path

        # Call the parent constructor AFTER all the attributes are initialized
        super().__init__(

            # Table name for this class
            ExecutionInfo.TABLE_NAME,

            # Foreign key attribute with its existing value
            {ExecutionInfo.FOREIGN_PARENT_JOB: parent_job})
        
