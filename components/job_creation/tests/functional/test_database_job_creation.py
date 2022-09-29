from datetime import datetime
import logging
import pytest

# check if environment variable is set, exit in error if it's not
from ....common.python.util.sys_util import SysUtil
SysUtil.ensure_env_var_set("COSIMS_DB_HTTP_API_BASE_URL")

from ....common.python.database.model.job.fsc_rlie_job import FscRlieJob
from ....common.python.database.model.job.job_priority import JobPriority
from ....common.python.database.model.job.job_status import JobStatus
from ...python.job_creation import JobCreation
from ....common.python.util.log_util import temp_logger
from ....common.python.util.fsc_rlie_job_util import FscRlieJobUtil
from ....common.python.database.rest.stored_procedure import StoredProcedure


@pytest.fixture
def empty_database():
    StoredProcedure.empty_database(temp_logger.debug)


def test_get_unique_jobs(empty_database):
    """Test that jobs presence in database is correctly verified."""

    # Variables definition
    existing_job_number = 5
    job_to_test_number = 2
    year = 2019
    existing_jobs_l1c_id = []
    jobs_to_test = []

    # Create jobs to store in DataBase
    for i in range(existing_job_number):
        job = FscRlieJob(
                id=str(i),
                nrt=True,
                tile_id="32TLR",
                l1c_id=str(i),
                l1c_path="test_path",
                l1c_cloud_cover=0,
                l1c_snow_cover=0,
                fsc_path="/test/path/fsc/"+str(i),
                rlie_path="/test/path/rlie/"+str(i),
                l2a_status="pending",
                measurement_date=datetime(year, i+1, 14, 6, 0),
                l1c_esa_publication_date=datetime(year, i+1, 15, 7, 0),
                l1c_dias_publication_date=datetime(year, i+1, 16, 8, 0),
            )

        # Keep trace of the existing jobs L1C id
        existing_jobs_l1c_id.append(job.l1c_id)

        # Create jobs to check if they exists in DataBase
        if i < job_to_test_number:
            jobs_to_test.append(
                FscRlieJob(
                    id=str(i + existing_job_number),
                    nrt=True,
                    tile_id="32TLR",
                    l1c_id=str(i) if i == 0 else str(i + existing_job_number),
                    l1c_path="test_path",
                    l1c_cloud_cover=0,
                    l1c_snow_cover=0,
                    fsc_path="/test/path/fsc/"+str(i + existing_job_number),
                    rlie_path="/test/path/rlie/"+str(i + existing_job_number),
                    l2a_status="pending",
                    measurement_date=datetime(year, i+1, 14 if i==0 else 13, 6, 0),
                    l1c_esa_publication_date=datetime(year, i+1, 15, 7, 0),
                    l1c_dias_publication_date=datetime(year, i+1, 16, 8, 0),
                )
            )

        # Always a near real-time context
        job.nrt = True

        # Set the log level used for this next job execution.
        job.next_log_level =  logging.getLevelName("DEBUG")

        # Insert the job (fsc_rlie + parent job) into the database
        job.post(post_foreign=True)

        # Set the job status to initialized
        job.post_new_status_change(JobStatus.initialized)

    
    # Call function to test
    unique_jobs = FscRlieJobUtil.get_unique_jobs(jobs_to_test, None, FscRlieJob, 1, temp_logger)

    # Ensure only one job is unique
    assert len(unique_jobs) == job_to_test_number - 1

    # Ensure the correct job is considered unique
    for unique_job in unique_jobs:
        assert unique_job.l1c_id not in existing_jobs_l1c_id, \
            "Error : unique job L1C id '%s' already exists in DataBase!" %unique_job.l1c_id
