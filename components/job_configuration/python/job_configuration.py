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
from ...common.python.util.exceptions import CsiInternalError
from ...common.python.util.resource_util import ResourceUtil
from ...common.python.util.esa_util import EsaUtil
from ...common.python.util.log_util import temp_logger

# check if environment variable is set, exit in error if it's not
from ...common.python.util.sys_util import SysUtil
SysUtil.ensure_env_var_set("COSIMS_DB_HTTP_API_BASE_URL")
SysUtil.ensure_env_var_set("CSI_SIP_DATA_BUCKET")



class JobConfiguration(LoopedJob):
    '''
    The Job configuration module is in charge of requesting new jobs from the database 
    and configuring them.
    '''

    # Path on disk of the configuration file
    __CONFIG_PATH = ResourceUtil.for_component(
        'job_configuration/config/job_configuration.yml')

    # Time in seconds between two requests to the database for new jobs.
    __SLEEP = None

    # Initial log level. Can be modified later by the operator for e.g. debugging jobs.
    __LOG_LEVEL = None


    @staticmethod
    def read_config_file():
        '''Read the configuration file.'''
        with open(JobConfiguration.__CONFIG_PATH, 'r',encoding='UTF-8') as stream:
            contents = yaml.safe_load(stream)
            JobConfiguration.__SLEEP = (
                contents['sleep'])
            JobConfiguration.__LOG_LEVEL = (
                logging.getLevelName(contents['log_level']))
            EsaUtil.PARALLEL_REQUESTS = (
                contents['esa_parallel_requests'])

        # Overload loop's sleep value with 'system_parameters' table value
        # if database is instanciated.
        try:
            JobConfiguration.__SLEEP = SystemPrameters().get(
                temp_logger.debug).job_configuration_loop_sleep
        except Exception:
            pass

    @staticmethod
    def start(*args, **kwargs):
        '''Start execution in an infinite loop.'''

        LoopedJob.static_start(
            job_name='job-configuration', 
            job_sub_type=JobConfiguration, 
            next_log_level=JobConfiguration.__LOG_LEVEL, 
            loop_sleep=JobConfiguration.__SLEEP,
            repeat_on_fail=True)

    def looped_start(self, *args):
        '''
        Start job execution, wrapped by OtherJob.wrapped_start
        Request the database every n minutes for new jobs and configure them.
        '''

        # Use the staticmethod start()
        if not self.logger:
            raise Exception('Logger must be initialized.')

        for job_type in JobTypes.get_job_type_list(self.logger):

            self.logger.info(f'Configuration service loop for {job_type.JOB_NAME} jobs')

            # Find all jobs with status initialized
            jobs = job_type.get_jobs_with_last_status(
                job_type, JobStatus.initialized, logger_func=self.logger.debug)

            # Find all jobs with status configured
            configured_jobs = job_type.get_jobs_with_last_status(
                job_type, JobStatus.configured, logger_func=self.logger.debug)

            # Sort configured jobs to avoid time series configuration issues
            configured_jobs.sort(key=lambda job: job.id)

            # Exit if no jobs
            if not jobs and not configured_jobs:
                self.logger.info(f'No new {job_type.JOB_NAME} jobs to configure')
                continue

            self.logger.info(f'Configure {len(jobs)} {job_type.JOB_NAME} jobs')

            # Perform a first configuration on the batch of jobs
            jobs = job_type.configure_batch_jobs(jobs, self.logger)

            # Add configured jobs to the list of jobs to be updated
            jobs = configured_jobs + jobs

            for job in jobs:

                # Determine in which status should be applied to the job : 
                #  - 'JobStatus.configured', if all the requirements are not met 
                #       (an other job should complete its processing for instance)
                #  - 'JobStatus.ready', if the job can be processed.
                # and perform additional configuration steps if needed.
                job, status_to_set, message = job.configure_single_job(self.logger)


                if job not in configured_jobs or status_to_set == JobStatus.ready:
                    # Update the job in the database if its status has been changed 
                    # from 'initialized' to 'ready', or if it has been changed to 'ready'.
                    job.patch(patch_foreign=True, logger_func=self.logger.debug)

                # Update job status
                if status_to_set == JobStatus.internal_error:
                    job.error_raised = True
                    job.post_new_status_change(JobStatus.internal_error, error_subtype=CsiInternalError, error_message=str(message))
                else:
                    if job not in configured_jobs:
                        job.post_new_status_change(JobStatus.configured)
                    if status_to_set == JobStatus.ready:
                        job.post_new_status_change(JobStatus.ready)
                    if status_to_set == JobStatus.cancelled:
                        if message is not None:
                            job.post_new_status_change(JobStatus.cancelled, error_message=str(message))
                        else:
                            job.post_new_status_change(JobStatus.cancelled)


# Static call: read the configuration file
JobConfiguration.read_config_file()
