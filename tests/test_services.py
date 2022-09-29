from datetime import datetime
import logging
import time

# check if environment variable is set, exit in error if it's not
from ..components.common.python.util.sys_util import SysUtil
SysUtil.ensure_env_var_set("COSIMS_DB_HTTP_API_BASE_URL")

from ..components.common.python.util.log_util import temp_logger
from ..components.common.python.util.fsc_rlie_job_util import FscRlieJobUtil
from ..components.common.python.database.rest.stored_procedure import StoredProcedure
from ..components.job_creation.python.dias_request import DiasRequest
from ..components.common.python.util.eea39_util import Eea39Util
from ..components.common.python.database.model.job.job_status import JobStatus
from ..components.common.python.database.model.job.l2a_status import L2aStatus
from ..components.common.python.database.model.job.fsc_rlie_job import FscRlieJob
from ..components.common.python.database.model.job.maja_mode import MajaMode
from ..components.common.python.database.model.job.other_job import OtherJob


def test_services_workflow():
    """Test job creation, configuration, and execution workflow."""

    # Variables definition
    fk_parent_job_name = "test-fk-parent"
    tile_id = "32TLR"
    geometry = Eea39Util.get_wkt(tile_id=tile_id)
    date_min = None
    date_max = datetime.utcnow()
    max_requested_pages = 5
    jobs_to_create = []
    job_number = 2


    # Get or create a parent job
    fk_parent_job = OtherJob.get_or_post(fk_parent_job_name)


    # Request FSC/RLIE jobs from new L1C products in the DIAS catalogue
    jobs = DiasRequest(temp_logger).request(
        geometry,
        date_min,
        date_max,
        max_requested_pages=max_requested_pages)

    # Keep only the selected EEA39 tile
    jobs = [j for j in jobs if j.tile_id == tile_id]

    # Ensure enough jobs were found
    assert len(jobs) >= job_number, \
        "Warning : not enough jobs to be created found in the requested Dias "\
        "pages (%s)" %max_requested_pages

    # Keep only the right number of jobs
    for i in range(job_number):
        jobs_to_create.append(jobs.pop())
    
    # post jobs to DataBase
    for job in jobs_to_create:

        # Always a near real-time context
        job.nrt = True

        # Set L2A product status 
        job.l2a_status = L2aStatus.pending

        # Set the log level used for this next job execution.
        job.next_log_level = logging.getLevelName("DEBUG")

        # Insert the job (fsc_rlie + parent job) into the database
        job.post(post_foreign=True, logger_func=temp_logger.debug)

        # Set the job status to initialized
        job.post_new_status_change(JobStatus.initialized)

    # Wait for services to operate
    time.sleep(10)

    # Find all jobs with status being in the RUNNING_STATUS list
    running_jobs = StoredProcedure.jobs_with_last_status(
        FscRlieJob, FscRlieJobUtil.RUNNING_STATUS, logger_func=temp_logger.debug)
    # Find the only job with status being READY (returned in a list)
    ready_jobs = StoredProcedure.jobs_with_last_status(
        FscRlieJob, JobStatus.ready, temp_logger.debug)
    # Find the only job with status being CONFIGURED (returned in a list)
    configured_jobs = StoredProcedure.jobs_with_last_status(
        FscRlieJob, JobStatus.configured,  temp_logger.debug)

    # Sort running jobs by measurement dates
    running_jobs.sort(key=lambda job: job.measurement_date)

    # Ensure we retreived correct number of jobs
    assert len(running_jobs) == job_number

    # Ensure jobs are correctly configured in the DataBase
    for i in range(len(running_jobs)):
        if i == 0:
            assert running_jobs[i].id == ready_jobs[0].id, \
                "Error : highest priority job's status is not READY!"
            assert running_jobs[i].maja_mode == MajaMode.init, \
                "Error : highest priority job's MAJA mode is %s instead of INIT" \
                %running_jobs[i].maja_mode
        elif i == 1:
            assert running_jobs[i].id == configured_jobs[0].id, \
                "Error : lowest priority job's status is not CONFIGURED!"
            assert running_jobs[i].maja_mode == None, \
                "Error : lowest priority job's MAJA mode is %s instead of NONE" \
                %running_jobs[i].maja_mode
            

