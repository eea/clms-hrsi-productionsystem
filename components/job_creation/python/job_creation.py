'''
_summary_

:raises Exception: _description_
'''
import logging
import yaml

from ...common.python.database.model.job.job_status import JobStatus
from ...common.python.database.model.job.job_types import JobTypes
from ...common.python.database.model.job.looped_job import LoopedJob
from ...common.python.database.model.job.system_parameters import SystemPrameters
from ...common.python.util.log_util import temp_logger
from ...common.python.util.request_util import RequestUtil
from ...common.python.util.resource_util import ResourceUtil


class JobCreation(LoopedJob):
    '''
    Monitor for new input products products in the DIAS catalog in a
    Near Real-Time (NRT) context.
    '''

    # Path on disk of the configuration file.
    __CONFIG_PATH = ResourceUtil.for_component(
        'job_creation/config/job_creation.yml')

    # Time in seconds between two requests to the DIAS for new products.
    __SLEEP = None

    # Log levels.
    # The log level for the job creation is fixed.
    # The initial log level for the jobs can be modified later
    # by the operator for e.g. debugging jobs.
    __JOB_CREATION_LOG_LEVEL = None
    __JOB_LOG_LEVEL = None

    # Number of parallel requests to the internal database to find existing input products products.
    __INTERNAL_DATABASE_PARALLEL_REQUESTS = 1


    @staticmethod
    def read_config_file():
        '''Read the configuration file.'''
        with open(JobCreation.__CONFIG_PATH, 'r', encoding='UTF-8') as stream:
            contents = yaml.safe_load(stream)
            JobCreation.__SLEEP = (
                contents['sleep'])
            JobCreation.__JOB_CREATION_LOG_LEVEL = (
                logging.getLevelName(contents['job_creation_log_level']))
            JobCreation.__JOB_LOG_LEVEL = (
                logging.getLevelName(contents['job_log_level']))
            JobCreation.__INTERNAL_DATABASE_PARALLEL_REQUESTS = (
                contents['internal_database_parallel_requests'])
            RequestUtil.PARALLEL_REQUESTS = (
                contents['dias_parallel_requests'])

        # Overload loop's sleep value with 'system_parameters' table value
        #  if database is instanciated.
        try:
            JobCreation.__SLEEP = SystemPrameters().get(
                temp_logger.debug).job_creation_loop_sleep
        except Exception:
            pass

    @staticmethod
    def start(*args, **kwargs):
        '''Start execution in an infinite loop.'''

        LoopedJob.static_start(
            job_name='job-creation',
            job_sub_type=JobCreation,
            next_log_level=JobCreation.__JOB_CREATION_LOG_LEVEL,
            loop_sleep=JobCreation.__SLEEP,
            repeat_on_fail=True)

    def looped_start(self, *args):
        '''
        Start job execution, wrapped by OtherJob.wrapped_start
        Request the DIAS catalog every n minutes for new input products and save them.
        '''

        # Use the staticmethod start()
        if not self.logger:
            raise Exception('Logger must be initialized.')

        for job_type in JobTypes.get_job_type_list(self.logger):

            self.logger.info(f'Creation service loop for {job_type.JOB_NAME} jobs')

            # Find all the jobs of a given type that should be indexed in the
            # database. 'jobs' is the list of jobs which will be processed by
            # the system. 'reprocessed_jobs' is a potential list of old jobs
            # which are the consequence of a new publication of older products,
            # that we won't process, but that we still want to keep track of.
            jobs, reprocessed_jobs = job_type.get_jobs_to_create(
                JobCreation.__INTERNAL_DATABASE_PARALLEL_REQUESTS,
                self.logger
            )

            # Avoid None-related errors if 'reprocessed_jobs' is not set
            if reprocessed_jobs is None:
                reprocessed_jobs = []

            if not jobs:
                self.logger.info(f'No new input products products for '\
                    f'"{job_type.JOB_NAME}"')
                continue

            self.logger.info(f'Create {len(jobs)} {job_type.JOB_NAME} jobs')

            # TODO batch insert jobs (all at once) in the database.
            for job in jobs:

                # Set up job before insertion in database
                job.job_pre_insertion_setup(reprocessed_job=False)

                # Set the log level used for this next job execution.
                job.next_log_level = JobCreation.__JOB_LOG_LEVEL

                # Insert the job and its parent into the database
                job_response = job.post(post_foreign=True, logger_func=self.logger.debug)

                # Set the job status to initialized
                status_response = job.post_new_status_change(JobStatus.initialized)

                # If DataBase is not reachable -> break the loop to avoid missing jobs
                if job_response is None or status_response is None:
                    return

                # Update last inserted job stored value if database could be reached
                job_type.LAST_INSERTED_JOB = job


            for reprocessed_job in reprocessed_jobs:

                # Set up job before insertion in database
                reprocessed_job.job_pre_insertion_setup(reprocessed_job=True)

                # Set the log level used for this next job execution.
                reprocessed_job.next_log_level = JobCreation.__JOB_LOG_LEVEL

                # Insert the job and its parent into the database
                job_response = reprocessed_job.post(post_foreign=True,
                                                    logger_func=self.logger.debug)

                # Set the job status to cancelled to not process the job
                status_response = reprocessed_job.post_new_status_change(
                    JobStatus.cancelled,
                    error_message="Unprocessed job as it's part of a reprocessing campaign."
                    )

                # If DataBase is not reachable -> break the loop to avoid missing jobs
                if job_response is None or status_response is None:
                    return


# Static call: read the configuration file
JobCreation.read_config_file()
