from datetime import datetime
import pytest
import logging

# check if environment variable is set, exit in error if it's not
from ...python.util.sys_util import SysUtil
SysUtil.ensure_env_var_set("COSIMS_DB_HTTP_API_BASE_URL")

try:
    from ...python.util.fsc_rlie_job_util import FscRlieJobUtil
except SystemExit:
    pass

from ...python.database.model.job.fsc_rlie_job import FscRlieJob
from ...python.database.model.job.other_job import OtherJob
from ...python.database.model.job.job_status import JobStatus
from ...python.database.rest.stored_procedure import StoredProcedure
from ...python.util.log_util import temp_logger


@pytest.fixture
def empty_database():
    StoredProcedure.empty_database(temp_logger.debug)

def test_find_previous_temporal_job_no_l2a_restriction(empty_database):
    """Test that dependencies between jobs are computed correctly."""

    # Variables definition
    fk_parent_job_name = "test-fk-parent"
    fsc_rlie_job_number = 6
    year = 2019
    fsc_rlie_jobs = []

    # Get or create a parent job
    fk_parent_job = OtherJob.get_or_post(fk_parent_job_name)

    # Create jobs to store in DataBase
    for i in range(fsc_rlie_job_number):
        fsc_rlie_job = FscRlieJob(
                id=str(i),
                nrt=True,
                tile_id="32TLR0" if i%2==0 else "32TLR1",
                l1c_id=str(i),
                l1c_path="test_path",
                l1c_cloud_cover=0,
                l1c_snow_cover=0,
                fsc_path="/test/path/fsc/"+str(i),
                rlie_path="/test/path/rlie/"+str(i),
                l2a_status="pending",
                measurement_date=datetime(year + (i//12), (i%12)+1, 14, 6, 0),
                l1c_esa_publication_date=datetime(year + (i//12), (i%12)+1, 15, 7, 0),
                l1c_esa_creation_date=datetime(year + (i//12), (i%12)+1, 15, 7, 0),
                l1c_dias_publication_date=datetime(year + (i//12), (i%12)+1, 16, 8, 0),
            )

        # Keep trace of the created FSC/RLIE jobs
        fsc_rlie_jobs.append(fsc_rlie_job)

        # Always a near real-time context
        fsc_rlie_job.nrt = True

        # Set the log level used for this next job execution
        fsc_rlie_job.next_log_level = logging.getLevelName("DEBUG")

        # Insert the job (fsc_rlie + parent job) into the database
        fsc_rlie_job.post(post_foreign=True, logger_func=temp_logger.debug)

        # Set the job status to initialized
        fsc_rlie_job.post_new_status_change(JobStatus.initialized)

        # Call function to test
        previous_job = FscRlieJobUtil.find_previous_temporal_job_no_l2a_restriction(
            fsc_rlie_job, 
            FscRlieJob(),
            logger_func=temp_logger
            )

        if i in [0,1]:
            assert previous_job is None, \
                "Error : dependencies computed are not correct, found dependent "\
                "job (id=%s) instead of none!" %previous_job.id
        else:
            assert previous_job.id == fsc_rlie_jobs[i-2].id, \
                "Error : dependencies computed are not correct, found dependent "\
                "job with id = %s instead of %s !" %(
                    previous_job.id, 
                    fsc_rlie_jobs[i-2].id
                    )
