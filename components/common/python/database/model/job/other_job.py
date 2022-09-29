from threading import Lock
import time

from .child_job import ChildJob
from ....util.log_util import temp_logger


class OtherJob(ChildJob):
    '''
    Any simple job other than FSC/RLIE, PSA or ARLIE, e.g. the job creation, 
    configuration and execution processes.
    Used only to attach log messages to these processes.
    '''

    # Database table name
    TABLE_NAME = "other_jobs"

    def __init__(self, name=''):

        # Call the parent constructor AFTER all the attributes are initialized
        super().__init__(OtherJob.TABLE_NAME)

        # Save job name
        self.name = name

        # Set dummy values for unused mandatory fields
        self.tile_id = ''

    __lock_get_or_post = Lock()

    @staticmethod
    def get_or_post(name, sub_type=None, repeat_on_fail=False):
        '''
        Create and return a job with the given name. 
        Save it into the database, if it does not already exist. 
        Duplicate entries are identified only by the name.
        
        :param sub_type: create a job instance from a child class.
        '''

        # Enable first loop iteration without checking for conditions
        first_iteration = True

        # Variable to check if job was not sucessfully created
        job_not_created = True
        
        # Default sub type = self
        if not sub_type:
            sub_type = OtherJob

        # Name must be defined
        if not name:
            raise Exception('Job name is missing')

        # Use a Lock section so multiple threads will not insert duplicate jobs
        # at the same time. But it does not work in case of multiple processes.
        with OtherJob.__lock_get_or_post:

            # Loop while job was not sucessfully created
            while first_iteration or (job_not_created and repeat_on_fail):

                # First loop iteration is running
                first_iteration = False

                # Get existing jobs with the same name
                other_jobs = (
                    sub_type().name_eq(name).
                    get(logger_func=temp_logger.debug))

                # Return the existing job
                if other_jobs:
                    return other_jobs[0]

                # Else create the Python instance
                job = sub_type(name)

                # Insert it into the database then return it
                # post_foreign=True to create the job parent and child entries in the database.
                response = job.post(post_foreign=True, logger_func=temp_logger.debug)
                
                # Check if job was not sucessfully created
                if response is not None:
                    job_not_created = False
                else:
                    temp_logger.error("Database not available, service won't be able to start."\
                        " Attempting to connect to database...")
                    time.sleep(5)

            return job
