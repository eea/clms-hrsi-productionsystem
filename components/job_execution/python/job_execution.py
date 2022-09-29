'''
_summary_

:raises Exception: _description_
:return: _description_
:rtype: _type_
'''
import logging
import nomad
import yaml

from ...common.python.database.model.job.job_status import JobStatus
from ...common.python.database.model.job.job_types import JobTypes
from ...common.python.database.model.job.looped_job import LoopedJob
from ...common.python.database.model.job.system_parameters import SystemPrameters
from ...common.python.util.log_util import temp_logger
from ...common.python.util.resource_util import ResourceUtil

# check if environment variable is set, exit in error if it's not
from ...common.python.util.sys_util import SysUtil
SysUtil.ensure_env_var_set("COSIMS_DB_HTTP_API_BASE_URL")



class JobExecution(LoopedJob):
    '''
    The Job execution module is in charge of requesting new jobs from the database
    and executing them.
    '''

    # Path on disk of the configuration file
    __CONFIG_PATH = ResourceUtil.for_component(
        'job_execution/config/job_execution.yml')

    # Time in seconds between two requests to the database for new jobs.
    __SLEEP = None

    # Initial log level. Can be modified later by the operator for e.g. debugging jobs.
    __LOG_LEVEL = None

    @staticmethod
    def read_config_file():
        '''Read the configuration file.'''
        with open(JobExecution.__CONFIG_PATH, 'r', encoding='UTF-8') as stream:
            contents = yaml.safe_load(stream)
            JobExecution.__SLEEP = (
                contents['sleep'])
            JobExecution.__LOG_LEVEL = (
                logging.getLevelName(contents['log_level']))

        # Overload loop's sleep value with 'system_parameters' table value
        #  if database is instanciated.
        try:
            JobExecution.__SLEEP = SystemPrameters().get(
                temp_logger.debug).job_execution_loop_sleep
        except Exception:
            pass

    @staticmethod
    def start(*args, **kwargs):
        '''Start execution in an infinite loop.'''

        LoopedJob.static_start(
            job_name='job-execution',
            job_sub_type=JobExecution,
            next_log_level=JobExecution.__LOG_LEVEL,
            loop_sleep=JobExecution.__SLEEP,
            repeat_on_fail=True)

    def looped_start(self, *args):
        '''
        Start job execution, wrapped by OtherJob.wrapped_start
        Request the database every n minutes for new jobs and execute them.
        '''

        # Use the staticmethod start()
        if not self.logger:
            raise Exception('Logger must be initialized.')

        for job_type in JobTypes.get_job_type_list(self.logger):

            self.logger.info(f'Execution service loop for {job_type.JOB_NAME} jobs')

            # New jobs
            jobs = []

            jobs += job_type.get_jobs_with_last_status(
                job_type, JobStatus.ready, logger_func=self.logger.debug)

            # Exit if no jobs
            if not jobs:
                self.logger.info(f'No new {job_type.JOB_NAME} jobs to execute')
                continue

            # Sort jobs by priority (first nrt, then delayed and reprocessing)
            if jobs[0].priority:
                jobs.sort(key=lambda job: job.priority)

            # For now, iterate over each job
            for job in jobs:

                return_code = self.execute_job(job, job_type)
                # If a code 1 is returned, it means that the loop should be stopped
                if return_code == 1:
                    continue


    def execute_job(self, job, job_type):
        '''Execute a job.'''

        try:

            self.logger.info(
                f'Remote execution of the {job_type.JOB_NAME} job: {job.id}')

            self.logger.info('Request a new Nomad job')

            nomad_client = nomad.Nomad()
            try:
                response_dict = nomad_client.job.dispatch_job(
                    job_type.NOMAD_JOB_NAME,
                    meta={"job_id": str(job.id), "job_unique_id": str(job.unique_id)}
                )

            except nomad.api.exceptions.URLNotFoundNomadException as err:
                self.logger.error('URLNotFoundNomadException when trying to call the nomad function:')
                self.logger.error(f'  nomad_client.job.dispatch_job({job_type.NOMAD_JOB_NAME},meta={{job_id: '
                                  f'"{job.id}", job_unique_id: "{job.unique_id}"}})')
                self.logger.error(f'  URLNotFoundNomadException: "{err}"')
                # At this point there is nothing we can do and successive call
                # to the command might fail too.

                # Notify that an error occured through jobs status
                job.post_new_status_change(
                    JobStatus.internal_error,
                    error_subtype='Nomad Command Error',
                    error_message=(
                        f'URLNotFoundNomadException when trying to call the nomad command: '
                        f'"{err}"')
                )
                return

            except nomad.api.exceptions.BaseNomadException as err:
                # If the error is caused by the nomad-server being done, we want to
                #  stop trying to execute jobs and wait for Nomad to be up again
                if "Failed to establish a new connection: [Errno 111] Connection refused" in str(err):
                    self.logger.warning("The Nomad server couldn't be reached, so we "\
                        "wait for it to be up again before attempting to execute new jobs")
                    # Return a specific code to notify that the execution loop should be stopped
                    return 1

                # If the error is caused by a timeout on a Nomad communication,
                # we want to restart the job.
                if "(connect timeout=5)" in str(err) or "(read timeout=5)" in str(err):
                    self.logger.error('Nomad Timeout Exception when trying to call the nomad function:')
                    self.logger.error(f'  nomad_client.job.dispatch_job({job_type.NOMAD_JOB_NAME},meta={{job_id: '\
                        f'"{job.id}", job_unique_id: "{job.unique_id}"}})')
                    self.logger.error(f'  Nomad Timeout Exception: "{err}"')

                    # Notify that an error occured through jobs status
                    job.post_new_status_change(
                        JobStatus.internal_error,
                        error_subtype='Nomad Command Timeout Error',
                        error_message=(
                            f'Nomad Timeout Exception when trying to call the nomad command: '
                            f'"{err}"')
                    )
                    job.post_new_status_change(
                        JobStatus.error_checked,
                        error_message="Error automatically checked as it's "\
                            "linked to a temporar Nomad communication issue."
                    )
                    job.post_new_status_change(JobStatus.ready)
                    return

                # Otherwise an other error occurred, and we should notify it
                self.logger.error('BaseNomadException when trying to call the nomad function:')
                self.logger.error(f'  nomad_client.job.dispatch_job({job_type.NOMAD_JOB_NAME},meta={{job_id: '\
                    f'"{job.id}", job_unique_id: "{job.unique_id}"}})')
                self.logger.error(f'  BaseNomadException: "{err}"')
                # At this point there is nothing we can do and successive call
                # to the command might fail too.

                # Notify that an error occured through jobs status
                job.post_new_status_change(
                    JobStatus.internal_error,
                    error_subtype='Nomad Command Error',
                    error_message=(
                        f'BaseNomadException when trying to call the nomad command: '
                        f'"{err}"')
                )
                return

            except Exception as err:
                self.logger.error('Nomad command unknown error when trying to call the nomad function:.')
                self.logger.error(f'  nomad_client.job.dispatch_job({job_type.NOMAD_JOB_NAME},meta={{job_id: '\
                    f'"{job.id}", job_unique_id: "{job.unique_id}"}})')
                self.logger.error(f'Error: {err}')

                # Notify that an error occured through jobs status
                job.post_new_status_change(
                    JobStatus.internal_error,
                    error_subtype='Nomad Command Unknown Error',
                    error_message=(
                        f'Nomad command raised an unknown error while calling it: '
                        f'"{err}"')
                )
                return

            self.logger.info('Nomad job created')

            # The dispatch ID is one item of the response dictionary sent back by Nomad
            dispatch_id = response_dict['DispatchedJobID']
            self.logger.debug(f'The nomad dispatch ID for this job is: {dispatch_id}')

            job.parent_job.nomad_id = dispatch_id
            job.patch(patch_foreign=True, logger_func=self.logger.debug)
            job.post_new_status_change(JobStatus.queued)


        # Clean temporary files
        finally:
            #os.remove(JobExecution.__WORKER_FINAL_PATH)
            pass


# Static call: read the configuration file
JobExecution.read_config_file()
JobExecution.start()
