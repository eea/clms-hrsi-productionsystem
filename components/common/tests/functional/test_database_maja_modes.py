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
from ...python.database.model.job.l2a_status import L2aStatus
from ...python.database.model.job.maja_mode import MajaMode
from ...python.database.rest.stored_procedure import StoredProcedure
from ...python.util.log_util import temp_logger


@pytest.fixture
def empty_database():
    StoredProcedure.empty_database(temp_logger.debug)

def test_maja_init_mode_setting(empty_database):
    """
    Test that MAJA 'init' mode is correctly affected to jobs being configured.
    """

    # Variables definition
    fk_parent_job_name = "test-fk-parent"
    fsc_rlie_job_number = 16
    right_year = 2019
    fsc_rlie_jobs = []

    # Get or create a parent job
    fk_parent_job = OtherJob.get_or_post(fk_parent_job_name)

    # Create jobs to store in DataBase
    #   - 1st job : the job we want to assign a maja mode to
    #   - 2nd job : a non-consecutive job measured before the one to configure
    #   - 3rd job : a consecutive job measured before the one to configure but 
    # with no L2A available 
    #   - 4th to 16th jobs : 13 jobs measured after the one to configure
    # with the 7 first being consecutive with each other but not with the job to 
    # configure and the 6 last being consecutives with each other and with the 
    # job to configure, but not with the 7 previous ones(ensure backward 
    # is not triggerred is such case)
    for i in range(fsc_rlie_job_number):
        if i ==1:
            year = right_year - 1
        elif i in list(range(3,10)):
            year = right_year + 1
        else:
            year = right_year

        fsc_rlie_job = FscRlieJob(
                id=str(i),
                nrt=True,
                tile_id="32TLR0",
                l1c_id=str(i),
                l1c_path="test_path",
                l1c_cloud_cover=0,
                l1c_snow_cover=0,
                l2a_path_out=None,
                fsc_path="/test/path/fsc/"+str(i),
                rlie_path="/test/path/rlie/"+str(i),
                l2a_status=L2aStatus.deleted if i == 2 else L2aStatus.pending,
                n_jobs_run_since_last_init=1,
                measurement_date=datetime(
                    year, 
                    1 if i < 3 else 2, 
                    25-i if i < 3 else i+1, 
                    6, 
                    0
                ),
                l1c_esa_publication_date=datetime(year, 1, 1, 7, 0),
                l1c_esa_creation_date=datetime(year, 1, 1, 7, 0),
                l1c_dias_publication_date=datetime(year, 1, 1, 8, 0),
            )

        # Keep trace of the created FSC/RLIE jobs
        fsc_rlie_jobs.append(fsc_rlie_job)

        # set job to test maja mode to None
        fsc_rlie_jobs[0].maja_mode = None

        # Always a near real-time context
        fsc_rlie_job.nrt = True

        # Set the log level used for this next job execution
        fsc_rlie_job.next_log_level = logging.getLevelName("DEBUG")

        # Insert the job (fsc_rlie + parent job) into the database
        fsc_rlie_job.post(post_foreign=True, logger_func=temp_logger.debug)

        # Set the job status to initialized ()
        fsc_rlie_job.post_new_status_change(JobStatus.initialized)

        # Call function to test
        FscRlieJobUtil.set_maja_mode(fsc_rlie_jobs[0], FscRlieJob, temp_logger)

        # Ensure the right MAJA mode has been set
        if i == 0:
            # If no job is present in database, no MAJA mode should be set, as we
            # want by default to initialize the first job in the database with a
            # Backward, and to do so it should wait for enough jobs to be created.
            assert fsc_rlie_jobs[0].maja_mode == None, \
                "Error : at loop %s, wrong mode set '%s' instead of '%s' !" %(
                    i,
                    fsc_rlie_jobs[0].maja_mode,
                    None
                )
        else:
            assert fsc_rlie_jobs[0].maja_mode == MajaMode.init, \
                "Error : at loop %s, wrong mode set '%s' instead of '%s' !" %(
                    i,
                    fsc_rlie_jobs[0].maja_mode,
                    MajaMode.init
                )


def test_maja_backward_mode_setting(empty_database):
    """
    Test that MAJA 'backward' mode is correctly affected to jobs being configured.
    """

    # Variables definition
    fk_parent_job_name = "test-fk-parent"
    fsc_rlie_job_number = 12
    year = 2019
    wrong_year = 2018
    fsc_rlie_jobs = []

    # Get or create a parent job
    fk_parent_job = OtherJob.get_or_post(fk_parent_job_name)

    # Create jobs to store in DataBase
    #   - 9 first jobs : 9 consecutive jobs allowing backward to run
    #   - 10th job : the job we want to assign a maja mode to
    #   - 11th job : a non-consecutive job measured before the one to configure
    #   - 12th job : a consecutive job measured before the one to configure but 
    # with no L2A available 
    for i in range(fsc_rlie_job_number):
        fsc_rlie_job = FscRlieJob(
                id=str(i),
                nrt=True,
                tile_id="32TLR0",
                l1c_id=str(i),
                l1c_path="/test_path/"+str(i),
                l1c_cloud_cover=0,
                l1c_snow_cover=0,
                l2a_path_out=None,
                fsc_path=f"/test/path/fsc/{i}/compo_0", # _0 required to flag the degraded quality
                rlie_path=f"/test/path/rlie/{i}/compo_0",
                l2a_status=L2aStatus.generated if i < 11 else L2aStatus.deleted,
                n_jobs_run_since_last_init=i,
                n_l2a_produced_since_last_init=i,
                measurement_date=datetime(
                    wrong_year if i == 10 else year, 
                    2 if i < 9 else 1, 
                    i+1 if i < 9 else 25-i, 
                    6, 
                    0
                ),
                l1c_esa_publication_date=datetime(year, 1, 1, 7, 0),
                l1c_esa_creation_date=datetime(
                    wrong_year if i == 10 else year, 
                    2 if i < 9 else 1, 
                    i+1 if i < 9 else 25-i, 
                    7, 
                    0
                ),
                l1c_dias_publication_date=datetime(year, 1, 1, 8, 0),
            )

        # Keep trace of the created FSC/RLIE jobs
        fsc_rlie_jobs.append(fsc_rlie_job)
        if i == 9:
            tested_job_l1c_id_ref_value = fsc_rlie_jobs[9].l1c_id
            tested_job_l1c_path_ref_value = fsc_rlie_jobs[9].l1c_path

        # Always a near real-time context
        fsc_rlie_job.nrt = True

        # Set the log level used for this next job execution
        fsc_rlie_job.next_log_level = logging.getLevelName("DEBUG")

        # Insert the job (fsc_rlie + parent job) into the database
        fsc_rlie_job.post(post_foreign=True, logger_func=temp_logger.debug)

        # Set the job status to initialized ()
        fsc_rlie_job.post_new_status_change(JobStatus.initialized)

        if i > 8:
            # Reset job to test maja mode, l1c id, and l1c path
            fsc_rlie_jobs[9].maja_mode = None
            fsc_rlie_jobs[9].l1c_id = tested_job_l1c_id_ref_value
            fsc_rlie_jobs[9].l1c_path = tested_job_l1c_path_ref_value

            # Call function to test
            FscRlieJobUtil.set_maja_mode(fsc_rlie_jobs[9], FscRlieJob, temp_logger)

            # Ensure the right MAJA mode has been set
            assert fsc_rlie_jobs[9].maja_mode == MajaMode.backward, \
                "Error : at loop %s, wrong mode set '%s' instead of '%s' !" %(
                i,
                fsc_rlie_jobs[9].maja_mode,
                MajaMode.backward
            )

            # TODO SHOULD : check how to configure job with right input for Backward
            #   and update test function of it

            # Expected jobs' id to have been used for configuration
            jobs_id_used_for_configuration = ["9", "0", "1", "2", "3", "4", "5", "6"]

            # Ensure l1c_id parameter has been correctly set
            assert fsc_rlie_jobs[9].l1c_id_list == ";".join(jobs_id_used_for_configuration), \
                "Error : at loop %s, wrong l1c_id set '%s' instead of '%s' !" %(
                i,
                fsc_rlie_jobs[9].l1c_id_list,
                ";".join(jobs_id_used_for_configuration))

            # Ensure l1c_path parameter has been correctly set
            assert fsc_rlie_jobs[9].l1c_path_list == ";".join([
                "/test_path/" + number 
                for number 
                in jobs_id_used_for_configuration
            ]), \
                "Error : at loop %s, wrong l1c_path set '%s' instead of '%s' !" %(
                i,
                fsc_rlie_jobs[9].l1c_path_list,
                ";".join([
                    "/test_path/" + number 
                    for number 
                    in jobs_id_used_for_configuration
                ]))


def test_maja_nominal_mode_setting(empty_database):
    """
    Test that MAJA 'nominal' mode is correctly affected to jobs being configured.
    """

    # Variables definition
    fk_parent_job_name = "test-fk-parent"
    fsc_rlie_job_number = 5
    year = 2019
    measurement_date_days = [1,1,2,2,5]
    dias_publication_date_days = [1,4,2,3,5]
    fsc_rlie_jobs = []

    # Get or create a parent job
    fk_parent_job = OtherJob.get_or_post(fk_parent_job_name)

    # Create jobs to store in DataBase
    for i in range(fsc_rlie_job_number):
        fsc_rlie_job = FscRlieJob(
                id=str(i),
                nrt=True,
                tile_id="32TLR0",
                l1c_id=str(i),
                l1c_path="test_path",
                l1c_cloud_cover=0,
                l1c_snow_cover=0,
                l2a_path_out=None if i == 4 else "test/path/l2a_out/"+str(i),
                fsc_path="/test/path/fsc/"+str(i),
                rlie_path="/test/path/rlie/"+str(i),
                l2a_status="pending",
                n_jobs_run_since_last_init=1,
                measurement_date=datetime(year + (i//12), 1, measurement_date_days[i], 6, 0),
                l1c_esa_publication_date=datetime(year + (i//12), 1, 1, 7, 0),
                l1c_dias_publication_date=datetime(year + (i//12), 1, dias_publication_date_days[i], 8, 0),
                l1c_esa_creation_date=datetime(year + (i//12), 1, dias_publication_date_days[i], 8, 0),
            )

        # Keep trace of the created FSC/RLIE jobs
        fsc_rlie_jobs.append(fsc_rlie_job)

        # Always a near real-time context
        fsc_rlie_job.nrt = True

        # Set the log level used for this next job execution
        fsc_rlie_job.next_log_level = logging.getLevelName("DEBUG")

        # Insert the job (fsc_rlie + parent job) into the database
        fsc_rlie_job.post(post_foreign=True, logger_func=temp_logger.debug)

        # Set the job status to initialized ()
        fsc_rlie_job.post_new_status_change(JobStatus.initialized)

        if i > 3:
            # set job to test maja mode to None
            fsc_rlie_jobs[i].maja_mode = None

            # Call function to test
            FscRlieJobUtil.set_maja_mode(fsc_rlie_jobs[i], FscRlieJob, temp_logger)

            # Ensure the right MAJA mode has been set
            assert fsc_rlie_jobs[i].maja_mode == MajaMode.nominal, \
                "Error : at loop %s, wrong mode set '%s' instead of '%s' !" %(
                i,
                fsc_rlie_jobs[i].maja_mode,
                MajaMode.nominal
            )

            # Ensure the right job has been selected as last valid L2A product
            assert fsc_rlie_jobs[i].job_id_for_last_valid_l2a == i-1, \
                "Error : at loop %s, wrong job id selected as last valid L2A product, "\
                "'%s' instead of '%s' !" %(
                i,
                fsc_rlie_jobs[i].job_id_for_last_valid_l2a,
                i-1
            )