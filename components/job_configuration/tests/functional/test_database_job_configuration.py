from datetime import datetime
import logging
import pytest

# check if environment variable is set, exit in error if it's not
from ....common.python.util.sys_util import SysUtil
SysUtil.ensure_env_var_set("COSIMS_DB_HTTP_API_BASE_URL")

from ....common.python.database.model.job.fsc_rlie_job import FscRlieJob
from ....common.python.database.model.job.psa_arlie_job import PsaArlieJob
from ....common.python.database.model.job.other_job import OtherJob
from ....common.python.database.model.job.job_status import JobStatus
from ....common.python.database.model.job.job_priority import JobPriority
from ....common.python.util.log_util import temp_logger
from ....common.python.util.fsc_rlie_job_util import FscRlieJobUtil
from ...python.job_configuration import JobConfiguration
from ....common.python.database.rest.stored_procedure import StoredProcedure


@pytest.fixture
def empty_database():
    StoredProcedure.empty_database(temp_logger.debug)


# def test_get_product_results_data(empty_database):
#     """Test that correct information are retreived to configure PSA/ARLIE jobs."""

#     # Variables definition
#     fk_parent_job_name = "test-fk-parent"
#     psa_arlie_job_number = 2
#     fsc_rlie_job_number = 24
#     year = 2019
#     psa_arlie_jobs = []
#     fsc_rlie_jobs = []

#     # Get or create a parent job
#     fk_parent_job = OtherJob.get_or_post(fk_parent_job_name)

#     # Jobs creation which will then be compared
#     for i in range(psa_arlie_job_number):
#         psa_arlie_jobs.append(
#             PsaArlieJob(
#                 request_id=str(i),
#                 product_type='PSA-WGS84' if i%2 == 0 else 'ARLIE',
#                 tile_id='32TLR',
#                 hydro_year=datetime(
#                     year, i+1, 14, 6, 0
#                     ).strftime('%Y-%m-%dT%H:%M:%S') 
#                     if i%2 == 0 
#                     else None,
#                 month=datetime(
#                     year, i+1, 14, 6, 0
#                     ).strftime('%Y-%m-%dT%H:%M:%S') 
#                     if i%2 != 0 
#                     else None
#             )
#         )

#     # Create jobs to store in DataBase
#     for i in range(fsc_rlie_job_number):
#         fsc_rlie_job = FscRlieJob(
#                 id=str(i),
#                 nrt=True,
#                 tile_id="32TLR",
#                 l1c_id=str(i),
#                 l1c_path="test_path",
#                 l1c_cloud_cover=0,
#                 l1c_snow_cover=0,
#                 fsc_path="/test/path/fsc/"+str(i),
#                 rlie_path="/test/path/rlie/"+str(i),
#                 l2a_status="pending",
#                 measurement_date=datetime(year + (i//12), (i%12)+1, 14, 6, 0),
#                 l1c_esa_publication_date=datetime(year + (i//12), (i%12)+1, 15, 7, 0),
#                 l1c_dias_publication_date=datetime(year + (i//12), (i%12)+1, 16, 8, 0),
#             )

#         # Keep trace of the created FSC/RLIE jobs
#         fsc_rlie_jobs.append(fsc_rlie_job)

#         # Always a near real-time context
#         fsc_rlie_job.nrt = True

#         # Set the log level used for this next job execution
#         fsc_rlie_job.next_log_level = logging.getLevelName("DEBUG")

#         # Insert the job (fsc_rlie + parent job) into the database
#         fsc_rlie_job.post(post_foreign=True, logger_func=temp_logger.debug)

#         # Set the job status to processed
#         fsc_rlie_job.post_new_status_change(JobStatus.initialized)
#         fsc_rlie_job.post_new_status_change(JobStatus.configured)
#         fsc_rlie_job.post_new_status_change(JobStatus.ready)
#         fsc_rlie_job.post_new_status_change(JobStatus.queued)
#         fsc_rlie_job.post_new_status_change(JobStatus.started)
#         fsc_rlie_job.post_new_status_change(JobStatus.pre_processing)
#         fsc_rlie_job.post_new_status_change(JobStatus.processing)
#         fsc_rlie_job.post_new_status_change(JobStatus.post_processing)
#         fsc_rlie_job.post_new_status_change(JobStatus.processed)
#         fsc_rlie_job.post_new_status_change(JobStatus.done)

#     # Call function to test
#     JobConfiguration().get_product_results_data(psa_arlie_jobs, temp_logger)

#     # Ensure the PSA/ARLIE jobs are well configured
#     for psa_arlie_job in psa_arlie_jobs:
#         if psa_arlie_job.product_type in ["PSA-WGS84", "PSA-LAEA"]:
#             # For PSA jobs the products used cover from May the 1st
#             #  to the 30th of September included
#             assert psa_arlie_job.first_product_measurement_date == fsc_rlie_jobs[4].measurement_date
#             assert psa_arlie_job.last_product_measurement_date == fsc_rlie_jobs[8].measurement_date
#             assert psa_arlie_job.input_paths == ";".join(
#                 [job.fsc_path 
#                 for job 
#                 in fsc_rlie_jobs[4:9]
#                 ])
#         else:
#             # For ARLIE jobs the products used are the ones mesaured within
#             #  the month of interest
#             assert psa_arlie_job.first_product_measurement_date == fsc_rlie_jobs[1].measurement_date
#             assert psa_arlie_job.last_product_measurement_date == fsc_rlie_jobs[1].measurement_date
#             assert psa_arlie_job.input_paths == fsc_rlie_jobs[1].rlie_path


def test_update_job_status_function_of_dependencies(empty_database):
    """
    Test that FSC/RLIE jobs status are correctly updated function of 
    their dependencies to other jobs.
    """

    # Variables definition
    fk_parent_job_name = "test-fk-parent"
    fsc_rlie_job_number = 6
    year = 2019
    status = [1,1,1,2,9,1,1]
    fsc_rlie_jobs = []
    job_dependencies = {}

    # Get or create a parent job
    fk_parent_job = OtherJob.get_or_post(fk_parent_job_name)

    # Create FSC/RLIE jobs to configure
    for i in range(fsc_rlie_job_number):
        fsc_rlie_job = FscRlieJob(
            id=str(i),
            nrt=True,
            tile_id="32TLR",
            l1c_id=str(i),
            l1c_path="test_path",
            l1c_cloud_cover=0,
            l1c_snow_cover=0,
            fsc_path="/test/path/fsc/"+str(i),
            rlie_path="/test/path/rlie/"+str(i),
            l2a_status="generated" if i == 4 else "pending",
            measurement_date=datetime(year + (i//12), (i%12)+1, 14, 6, 0),
            l1c_esa_publication_date=datetime(year + (i//12), (i%12)+1, 15, 7, 0),
            l1c_dias_publication_date=datetime(year + (i//12), (i%12)+1, 16, 8, 0),
            last_status_id=status[i],
        )

        # Keep trace of the created FSC/RLIE jobs
        fsc_rlie_jobs.append(fsc_rlie_job)

        # Always a near real-time context
        fsc_rlie_job.nrt = True

        # Set the log level used for this next job execution
        fsc_rlie_job.next_log_level = logging.getLevelName("DEBUG")

        # Insert the job (fsc_rlie + parent job) into the database
        fsc_rlie_job.post(post_foreign=True, logger_func=temp_logger.debug)

        # Set the job status
        if i == 4:
            fsc_rlie_job.post_new_status_change(JobStatus.initialized)
            fsc_rlie_job.post_new_status_change(JobStatus.configured)
            fsc_rlie_job.post_new_status_change(JobStatus.ready)
            fsc_rlie_job.post_new_status_change(JobStatus.queued)
            fsc_rlie_job.post_new_status_change(JobStatus.started)
            fsc_rlie_job.post_new_status_change(JobStatus.pre_processing)
            fsc_rlie_job.post_new_status_change(JobStatus.processing)
            fsc_rlie_job.post_new_status_change(JobStatus.post_processing)
            fsc_rlie_job.post_new_status_change(JobStatus.processed)
        elif i == 3:
            fsc_rlie_job.post_new_status_change(JobStatus.initialized)
            fsc_rlie_job.post_new_status_change(JobStatus.configured)
        else:
            fsc_rlie_job.post_new_status_change(JobStatus.initialized)

        # Add dependency for current job to the previous one in the list
        #  dependencies = {1:0, 2:1, 3:2, 5:4}
        if i > 0 and i != 4:
            job_dependencies[fsc_rlie_jobs[i].id] = fsc_rlie_jobs[i-1].id 

        # Call function to test on un-completed jobs
        if i != 4:
            computed_status = fsc_rlie_jobs[i].update_job_status_function_of_dependencies(
                fsc_rlie_jobs[i-1] if i != 0 else None,
                temp_logger
                )
        else:
            computed_status = JobStatus.processed

        # Set the status we expect the updated job to be assigned to :
        #  [ready, configured, configured, configured, processed, ready]
        if i in [0,5]:
            expected_job_status = JobStatus.ready
        elif i in [1,2,3]:
            expected_job_status = JobStatus.configured
        else:
            expected_job_status = JobStatus.processed

        # Ensure the computed status is the expected one
        assert computed_status == expected_job_status, "Error : wrong status set at "\
            "loop '%s' status set is '%s' instead of '%s' !" %(
                i, 
                computed_status, 
                expected_job_status
            )


def test_configure_fsc_rlie(empty_database):
    """
    Test that FSC/RLIE jobs status are correctly configured.
    """

    # Variables definition
    fk_parent_job_name = "test-fk-parent"
    fsc_rlie_job_number = 6
    year = 2019
    fsc_rlie_jobs = []
    configured_jobs = []
    job_dependencies = {}

    # Get or create a parent job
    fk_parent_job = OtherJob.get_or_post(fk_parent_job_name)

    # Create FSC/RLIE jobs to configure
    for i in range(fsc_rlie_job_number):
        fsc_rlie_job = FscRlieJob(
            id=str(i),
            nrt=True,
            tile_id="32TLR",
            l1c_id=str(i),
            l1c_path="test_path",
            l1c_cloud_cover=0,
            l1c_snow_cover=0,
            n_jobs_run_since_last_init=i,
            n_l2a_produced_since_last_init=i,
            l2a_path_out="/test/path/l2a/"+str(i),
            fsc_path="/test/path/fsc/"+str(i),
            rlie_path="/test/path/rlie/"+str(i),
            l2a_status="generated" if i == 4 else "pending",
            measurement_date=datetime(year + (i//12), (i%12)+1, 14, 6, 0),
            l1c_esa_publication_date=datetime(year + (i//12), (i%12)+1, 15, 7, 0),
            l1c_esa_creation_date=datetime(year + (i//12), (i%12)+1, 15, 7, 0),
            l1c_dias_publication_date=datetime(year + (i//12), (i%12)+1, 16, 8, 0),
        )

        # Keep trace of the created FSC/RLIE jobs
        fsc_rlie_jobs.append(fsc_rlie_job)

        # Always a near real-time context
        fsc_rlie_job.nrt = True

        # NRT level
        fsc_rlie_job.priority = JobPriority.nrt

        # Set the log level used for this next job execution
        fsc_rlie_job.next_log_level = logging.getLevelName("DEBUG")

        # Insert the job (fsc_rlie + parent job) into the database
        fsc_rlie_job.post(post_foreign=True, logger_func=temp_logger.debug)

        # Set the job status
        if i == 4:
            fsc_rlie_job.post_new_status_change(JobStatus.initialized)
            fsc_rlie_job.post_new_status_change(JobStatus.configured)
            fsc_rlie_job.post_new_status_change(JobStatus.ready)
            fsc_rlie_job.post_new_status_change(JobStatus.queued)
            fsc_rlie_job.post_new_status_change(JobStatus.started)
            fsc_rlie_job.post_new_status_change(JobStatus.pre_processing)
            fsc_rlie_job.post_new_status_change(JobStatus.processing)
            fsc_rlie_job.post_new_status_change(JobStatus.post_processing)
            fsc_rlie_job.post_new_status_change(JobStatus.processed)
        elif i == 3:
            fsc_rlie_job.post_new_status_change(JobStatus.initialized)
            fsc_rlie_job.post_new_status_change(JobStatus.configured)
            configured_jobs.append(fsc_rlie_job)
        else:
            fsc_rlie_job.post_new_status_change(JobStatus.initialized)

        # Reproduce the job_configuration service steps to test on un-completed jobs
        if i != 4:

            if fsc_rlie_job not in configured_jobs:
                # Set job name from the tile ID and date
                fsc_rlie_job.name = fsc_rlie_job.tile_id
                # if job.measurement_date and job.priority != JobPriority.nrt:
                fsc_rlie_job.name += '-%s' % fsc_rlie_job.measurement_date.strftime('%Y-%m-%d')


            # Determine in which status should be applied to the job : 
            #  - 'JobStatus.configured', if all the requirements are not met 
            #       (an other job should complete its processing for instance)
            #  - 'JobStatus.ready', if the job can be processed.
            # and perform additional configuration steps if needed.
            fsc_rlie_job, status_to_set, _ = fsc_rlie_job.configure_single_job(temp_logger)

            if fsc_rlie_job not in configured_jobs or status_to_set == JobStatus.ready:
                # Update the job in the database if its status has been changed 
                # from 'initialized' to 'ready', or if it has been changed to 'ready'.
                fsc_rlie_job.patch(patch_foreign=True, logger_func=temp_logger.debug)

            # Update job status
            if fsc_rlie_job not in configured_jobs:
                fsc_rlie_job.post_new_status_change(JobStatus.configured)
            if status_to_set == JobStatus.ready:
                fsc_rlie_job.post_new_status_change(JobStatus.ready)


        # Set the status we expect the updated job to be assigned to :
        #  [ready, configured, configured, configured, processed, ready]
        if i in [5]:
            expected_job_status = [JobStatus.ready]
        elif i in [0,1,2,3]:
            expected_job_status = [JobStatus.configured]
        else:
            expected_job_status = [JobStatus.processed]

        # Try to retreive updated job, with expected status
        updated_jobs = StoredProcedure.jobs_with_last_status(
            FscRlieJob,
            expected_job_status,
            logger_func=temp_logger.debug
        )

        updated_jobs = [job for job in updated_jobs if job.id == fsc_rlie_jobs[i].id]

        # Ensure the updated job has been found in DataBase with correct status
        assert len(updated_jobs) == 1, "Error : %s job(s) found with expected id "\
            "(%s) and status (%s), instead of 1!" %(
                len(updated_jobs),
                fsc_rlie_jobs[i].id,
                expected_job_status[0]
                )


def test_update_priority(empty_database):
    """Test that job priority is computed correctly."""

    # Variables definition
    job_number = 7
    year = 2019
    jobs = []

    # Jobs creation which will then be checked
    for i in range(job_number):
        jobs.append(
            FscRlieJob(
                id=str(i),
                nrt=False if i == 0 else True,
                tile_id="31TCJ",
                l1c_id=str(i),
                l1c_path="test_path",
                l1c_cloud_cover=0,
                l1c_snow_cover=0,
                fsc_path="/test/path/fsc/"+str(i),
                rlie_path="/test/path/rlie/"+str(i),
                l2a_status="pending",
                measurement_date=None if i == 1 else datetime(year, i+1, 14, 6, 0),
                l1c_esa_publication_date=None if i == 2 else datetime(year, i+1, 15 if i == 4 else 14, 7, 0),
                l1c_dias_publication_date=None if i == 3 else datetime(year, i+1, 14, 11 if i == 5 else 8, 0),
            )
        )

        # Call the function to test
        FscRlieJobUtil.update_priority(jobs)

        # Ensure jobs priority are updated properly
        if i == 0:
            assert jobs[i].priority == JobPriority.reprocessing , \
                "Error : job[%s] priority is not 'reprocessing'!" %i
        elif i < job_number-1:
            assert jobs[i].priority == JobPriority.delayed , \
                "Error : job[%s] priority is not 'delayed'!" %i
        else:
            assert jobs[i].priority == JobPriority.nrt , \
                "Error : job[%s] priority is not 'nrt'!" %i