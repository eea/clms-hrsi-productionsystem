'''
Module providing the monitoring of the jobs executions.
'''
import os
import time
import logging
from datetime import datetime, timedelta

import yaml
import nomad

from ...common.python.database.logger import Logger
from ...common.python.database.model.job.job_status import JobStatus
from ...common.python.database.model.job.job_types import JobTypes
from ...common.python.database.model.job.looped_job import LoopedJob
from ...common.python.database.rest.stored_procedure import StoredProcedure
from ...common.python.util.resource_util import ResourceUtil
from ...common.python.util.exceptions import CsiInternalError

# check if environment variable is set, exit in error if it's not
from ...common.python.util.sys_util import SysUtil
SysUtil.ensure_env_var_set("COSIMS_DB_HTTP_API_BASE_URL")




def raise_internal_error_if_env_not_set_or_empty(env_name: str):
    '''Check the presence and validity of the env variable'''
    if env_name not in os.environ:
        raise CsiInternalError(
            'Missing env var',
            f'{env_name} environment variable not found')
    if os.environ[env_name] == '':
        raise CsiInternalError(
            'Empty env var',
            f'{env_name} is set but contains an empty string')

def notify_database_of_error(logger: Logger, job, summary: dict = None, error: str = "None"):
    '''Notify in database that an error occurred in Nomad with a specific job'''

    if error == "SI software docker image pull error":
        error_subtype = error
        error_message = (
            f'The job {job.id} with status {JobStatus(job.last_status_id).name} '
            f'and dispatch {job.parent_job.nomad_id} seems to experience trouble '
            f'copying the SI software docker image from the DIAS into the worker.'
        )
        error_check_message = (
            "Error automatically checked as it's likely coming from DIAS network.")

    else :
        error_subtype = "Job inconsistency with Nomad"
        error_message = (
            f'The job {job.id} with status {JobStatus(job.last_status_id).name} '
            f'and dispatch {job.parent_job.nomad_id} is marked as working '
            f'but is not recognized by Nomad as expected, error : {error}, '
            f'the Nomad job summary being {summary}.'
        )
        error_check_message = "Error automatically checked as it's coming from Nomad."

    logger.error(error_message)

    # Make sure the broken Nomad job is not runnning anymore
    nomad_client = nomad.Nomad()
    try:
        _ = nomad_client.job.deregister_job(job.parent_job.nomad_id)
    except nomad.api.exceptions.URLNotFoundNomadException as nomad_error:
        if nomad_error.nomad_resp.status_code == 404:
            # HTTP 404 means that Nomad can't find a job with the given ID.
            # The job is not running anymore, we can move to the next step.
            pass
        else:
            # This is a bug.
            raise nomad_error

    job.post_new_status_change(
        JobStatus.internal_error,
        error_subtype=error_subtype,
        error_message=error_message
    )
    # Add delay after setting a new status to avoid status conflict at
    # database level when two requests are too close in terms of time range.
    time.sleep(2)
    job.post_new_status_change(
        JobStatus.error_checked,
        error_message=error_check_message
    )
    # Add delay after setting a new status to avoid status conflict at
    # database level when two requests are too close in terms of time range.
    time.sleep(2)
    job.post_new_status_change(JobStatus.ready)

def monitor_job(logger: Logger, job, job_type, job_status_history):
    '''Check if the system state for the job is OK'''

    nomad_client = nomad.Nomad()
    try:
        nomad_job_summary = nomad_client.job.get_summary(job.parent_job.nomad_id)

        logger.debug(f"The nomad job summary returned for job '{job.id}' is : {nomad_job_summary}")

        # Retrieve the Nomad job summary
        # (cf https://www.nomadproject.io/api/jobs.html#read-job-summary)
        nomad_job_status_dict = nomad_job_summary['Summary']['run-and-log']

        # If the Nomad job is in 'Running' state, we don't want to stop it,
        # even if Nomad also labelled it as 'Failed', it might re-start it
        # on its own.
        if (
            nomad_job_status_dict['Running'] == 1
            and nomad_job_status_dict['Failed'] == 1
        ):
            logger.warning(f"Job with ID '{job.id}' is labelled as "\
                "'Running' and 'Failed' at Nomad level, while its status in the "\
                f"database is '{JobStatus(job.last_status_id).name}', "\
                "it might be needed to re-process it!")

        # If the Nomad job is in an intermediate state
        elif (
            nomad_job_status_dict['Complete'] == 1
            or nomad_job_status_dict['Failed'] == 1
            or nomad_job_status_dict['Lost'] == 1
        ):
            # Check the job current status
            updated_job = job_type().job_id_in([job.id]).get(logger.debug)

            # The request response is a list -> return the first and unique element
            #  if it exists, else return None
            if isinstance(updated_job, list) and len(updated_job) > 0:
                updated_job = updated_job[0]

                # Get the updated job status history
                updated_job_status_history = StoredProcedure.job_status_history(
                    [updated_job.fk_parent_job_id],
                    logger_func=logger.debug
                )
            else:
                updated_job = None
                updated_job_status_history = None

            # If the job status hasn't changed since we fetched it in this script,
            #  and the last status change is greater than 5 min, we raise an error
            #  similarly as if Nomad didn't recognize the job.
            if (
                updated_job is not None
                and job.last_status_id == updated_job.last_status_id
                and len(job_status_history) == len(updated_job_status_history)
                and (
                    datetime.utcnow()
                    - datetime.strptime(updated_job.last_status_change_date, '%Y-%m-%dT%H:%M:%S.%f').replace(tzinfo=None)
                    > timedelta(minutes=5)
                )
            ):
                # Otherwise, if the Nomad status is 'Lost', 'Failed', or 'complete'
                # while the job in the database is still in one of the processing states,
                # we want to try to reprocess the job.

                # Log Nomad info on allocation
                # (cf https://www.nomadproject.io/api/allocations.html#read-allocation)
                allocations = nomad_client.job.get_allocations(job.parent_job.nomad_id)
                for allocation in allocations:
                    allocation_response = nomad_client.allocation.get_allocation(allocation["ID"])

                    # Ensure that the allocation_response has appropriate keys
                    if (
                        all(key in allocation_response.keys() for key in ["ID", "NodeID", "TaskStates"])
                        and "run-worker" in allocation_response["TaskStates"].keys()
                    ):
                        logger.warning(f"The nomad job allocation '%s' from job "\
                            f"'{job.id}' running on node '%s' seems to experience "\
                            f"trouble, its current status is :\n    %s" %(
                                allocation_response["ID"],
                                allocation_response["NodeID"],
                                allocation_response["TaskStates"]["run-worker"]
                            )
                        )
                    else:
                        logger.warning(f"The nomad job allocation from job "\
                            f"'{job.id}' seems to experience trouble, the allocation "\
                            f"seemed to return an empty dictonary :\n    {allocation_response}")

                notify_database_of_error(logger, job, summary=nomad_job_summary)


        # If the job is tuck in a processing state, because of poor network
        elif job.last_status_id in [JobStatus.queued.value, JobStatus.started.value, 
            JobStatus.pre_processing.value, JobStatus.processing.value]:

            # Set the timeout threshold value depending on the job status
            if job.last_status_id == JobStatus.processing.value:
                timeout_value = timedelta(minutes=270)
            elif job.last_status_id == JobStatus.queued.value:
                timeout_value = timedelta(minutes=60)
            else:
                timeout_value = timedelta(minutes=30)

            # Check the job current status
            updated_job = job_type().job_id_in([job.id]).get(logger.debug)

            # The request response is a list -> return the first and unique element
            #  if it exists, else return None
            if isinstance(updated_job, list) and len(updated_job) > 0:
                updated_job = updated_job[0]

                # Get the updated job status history
                updated_job_status_history = StoredProcedure.job_status_history(
                    [updated_job.fk_parent_job_id],
                    logger_func=logger.debug
                )
            else:
                updated_job = None
                updated_job_status_history = None

            # If the job status hasn't changed since we fetched it in this script,
            #  and the last status change is greater than a threshold value, we
            #  raise an error to notify that the job is likely to experience issue
            #  to fetch the SI software docker image.
            if (
                updated_job is not None
                and job.last_status_id == updated_job.last_status_id
                and len(job_status_history) == len(updated_job_status_history)
                and (
                    datetime.utcnow()
                    - datetime.strptime(updated_job.last_status_change_date, '%Y-%m-%dT%H:%M:%S.%f').replace(tzinfo=None)
                    > timeout_value
                )
            ):
                notify_database_of_error(logger, job, error="SI software docker image pull error")


    except nomad.api.exceptions.URLNotFoundNomadException as error:
        if error.nomad_resp.status_code == 404:
            # HTTP 404 means that Nomad can't find a job with the given ID.

            # The job is marked as working and its Nomad dispatch ID is set.
            # This job must be stuck in that state. If its status didn't change
            #  lately, consider to change it to "ready" so that the job execution
            # service can send it again to Nomad.

            # Check the job current status
            updated_job = job_type().job_id_in([job.id]).get(logger.debug)

            # The request response is a list -> return the first and unique element
            #  if it exists, else return None
            if isinstance(updated_job, list) and len(updated_job) > 0:
                updated_job = updated_job[0]

                # Get the updated job status history
                updated_job_status_history = StoredProcedure.job_status_history(
                    [updated_job.fk_parent_job_id],
                    logger_func=logger.debug
                )
            else:
                updated_job = None
                updated_job_status_history = None

            # Ensure the job status hasn't changed since we fetched it in this
            #  script, and the last status change is greater than 5 min.
            if (
                updated_job is not None
                and job.last_status_id == updated_job.last_status_id
                and len(job_status_history) == len(updated_job_status_history)
                and (
                    datetime.utcnow()
                    - datetime.strptime(updated_job.last_status_change_date, '%Y-%m-%dT%H:%M:%S.%f').replace(tzinfo=None)
                    > timedelta(minutes=5)
                )
            ):

                notify_database_of_error(logger, job, error="404 URLNotFoundNomadException")

        else:
            # This is a bug.
            logger.error(f"Unexpected error while trying to contact Nomad "\
                f"\nError : {error}")
            raise error



class Monitor(LoopedJob):
    '''
    The monitor module is in charge of monitoring for divergence between the
    database and the actual system state, raising errors and taking appropriate
    automatic actions.
    '''

    # Path on disk of the configuration file
    __CONFIG_PATH = ResourceUtil.for_component(
        'monitor/config/monitor.yml')

    # Time in seconds between two requests to the database for new jobs.
    __SLEEP = None

    # Initial log level. Can be modified later by the operator for e.g. debugging jobs.
    __LOG_LEVEL = None

    @staticmethod
    def read_config_file():
        '''Read the configuration file.'''
        with open(Monitor.__CONFIG_PATH, 'r', encoding='UTF-8') as stream:
            contents = yaml.safe_load(stream)
            Monitor.__SLEEP = (
                contents['sleep'])
            Monitor.__LOG_LEVEL = (
                logging.getLevelName(contents['log_level']))

    @staticmethod
    def start(*args, **kwargs):
        '''Start execution in an infinite loop.'''

        LoopedJob.static_start(
            job_name='monitor',
            job_sub_type=Monitor,
            next_log_level=Monitor.__LOG_LEVEL,
            loop_sleep=Monitor.__SLEEP,
            repeat_on_fail=True)

    def looped_start(self, *args):
        '''
        Start the monitoring, wrapped by OtherJob.wrapped_start
        Request the database every n minutes.
        '''

        # Use the staticmethod start()
        if not self.logger:
            raise Exception('Logger must be initialized.')

        logger = self.logger

        logger.info('')
        logger.info('----------------------------------------------------')
        logger.info('start job monitoring...')

        raise_internal_error_if_env_not_set_or_empty('CSI_NOMAD_SERVER_IP')
        raise_internal_error_if_env_not_set_or_empty('CSI_HTTP_API_INSTANCE_IP')


        for job_type in JobTypes.get_job_type_list(self.logger):

            self.logger.info(f'Monitor service loop for {job_type.JOB_NAME} jobs')

            # New jobs
            jobs = []
            jobs_status_history = {}

            status_with_worker_ownership = [
                JobStatus.queued,
                JobStatus.started,
                JobStatus.pre_processing,
                JobStatus.processing,
                JobStatus.post_processing
                ]
            jobs = job_type.get_jobs_with_last_status(
                job_type, status_with_worker_ownership, logger_func=self.logger.debug)

            # Exit if no jobs
            if not jobs:
                self.logger.info(f'There is no {job_type.JOB_NAME} job that is marked as working')
                continue

            # Get the jobs status history
            for job in jobs:
                jobs_status_history[job.id] = StoredProcedure.job_status_history(
                    [job.fk_parent_job_id],
                    logger_func=logger.debug
                )

            for job in jobs:
                monitor_job(logger, job, job_type, jobs_status_history[job.id])

        self.logger.info('job monitoring finished.')



# Static call: read the configuration file
Monitor.read_config_file()
