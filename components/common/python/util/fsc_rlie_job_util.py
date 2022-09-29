import sys
import os
import logging
import itertools
from datetime import timedelta
from multiprocessing.dummy import Pool
import yaml

from ..database.model.job.job_status import JobStatus
from ..database.model.job.job_priority import JobPriority
from ..database.model.job.l2a_status import L2aStatus
from ..database.model.job.maja_mode import MajaMode
from ..database.rest.stored_procedure import StoredProcedure
from ..database.model.job.system_parameters import SystemPrameters
from ..util.log_util import temp_logger
from ..util.esa_util import EsaUtil
from.resource_util import ResourceUtil

class FscRlieJobUtil(object):
    '''utility functions related to FSC RLIE scientific application'''

    # initialized status
    INITIALIZED_STATUS = [
        JobStatus.initialized
    ]

    # List of job status waiting to be executed
    RUNNING_STATUS = [
        JobStatus.configured,
        JobStatus.ready,
        JobStatus.queued,
        JobStatus.started,
        JobStatus.pre_processing,
        JobStatus.processing,
        JobStatus.post_processing
        ]

    # List of job status not waiting to be executed
    PROCESSING_COMPLETED_STATUS = [
        JobStatus.processed,
        JobStatus.start_publication,
        JobStatus.published,
        JobStatus.done,
        JobStatus.internal_error,
        JobStatus.external_error,
        JobStatus.error_checked,
        JobStatus.cancelled
        ]


    # Path on disk of the configuration file
    __CONFIG_PATH = ResourceUtil.for_component(
        'job_creation/config/job_creation.yml')

    # The initial log level for the FSC/RLIE jobs can be modified later by the
    #  operator for e.g. debugging jobs.
    __FSC_RLIE_LOG_LEVEL = None


    @staticmethod
    def read_config_file():
        '''Read the configuration file.'''
        with open(FscRlieJobUtil.__CONFIG_PATH, 'r') as stream:
            contents = yaml.safe_load(stream)
            FscRlieJobUtil.__FSC_RLIE_LOG_LEVEL = (
                logging.getLevelName(contents['job_log_level']))

    @staticmethod
    def find_previous_temporal_job_no_l2a_restriction(job, fsc_rlie_job_object, allow_codated_jobs=False, logger_func=None):
        '''
        Retreive previous job, with measurement date inferior to the one passed
        in argument, focusing on the same tile, and which didn't fail to
        generate an L2A product yet. (it can be a job being configured/run and
        which didn't attempt to produce any L2A yet, or a job which already
        successfully generated one)

        :param job: job instance for which we want to find the previous temporal job.
        :param fsc_rlie_job_object: FscRlieJob().
        :param allow_codated_jobs: boolean, to notify if codated jobs can be returned.
        :param logger_func: logger instance.
        '''

        # Determine if the job has been triggered by a backward reprocessing or not
        backward_triggered_job = False
        if job.reprocessing_context == "backward":
            backward_triggered_job = True

        # Call stored procedure to retreive the most recent job, with a measurement
        #  date inferior to the current job, focusing on the same tile, and which
        #  didn't fail to produce a L2A yet
        previous_jobs = StoredProcedure.get_last_job_with_usable_l2a(
            job.tile_id,
            job.measurement_date.strftime('%Y-%m-%dT%H:%M:%S'),
            job.l1c_esa_creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
            job.l1c_id,
            allow_codated_jobs=allow_codated_jobs,
            backward_triggered_job=backward_triggered_job,
            fsc_rlie_job_object=fsc_rlie_job_object,
            logger_func=logger_func.debug
            )
        previous_jobs = [previous_job for previous_job in previous_jobs \
                         if previous_job.measurement_date < job.measurement_date]
        # The request response is a list -> return the first and unique element
        #  if it exists, else return None
        if isinstance(previous_jobs, list) and len(previous_jobs) > 0:
            previous_job = previous_jobs[0]
        else:
            previous_job = None

        return previous_job

    @staticmethod
    def set_maja_mode(job, fsc_rlie_job_class, logger_func=None):
        '''
        Check Database to define which MAJA mode the job should run with :
            - Init : no recent product is available and we don't have data for backward.
            - Nominal : a recent L2A product is available.
            - Backward : process/reprocess an old product using a set of more recent L1C.

        :param job: job instance for which we want to set the MAJA mode.
        :param fsc_rlie_job_class: FscRlieJob.
        :param logger_func: logger instance.
        '''

        # Not overwritting MAJA mode if already set
        if job.maja_mode is not None:
            return

        # Number of job data (L1C) required by MAJA to be able to run a
        #  BACKWARD process.
        maja_backward_required_job_number = SystemPrameters().get(
            temp_logger.debug).maja_backward_required_job_number

        # Option to activate "backward" reprocessing of jobs, processed with
        #  MAJA "init" mode, or old ones.
        activate_backward_reprocessing = SystemPrameters().get(
            temp_logger.debug).activate_backward_reprocessing

        # Find the most recent job which produced a valid L2A. At this stage all
        # previous jobs focusing on the same tile should be done as we set MAJA
        # mode once configuration dependencies are completed.
        last_l2a_valid_job = FscRlieJobUtil.find_previous_temporal_job_no_l2a_restriction(
            job,
            fsc_rlie_job_class(),
            allow_codated_jobs=False,
            logger_func=logger_func
            )

        # Check if dependent job exists,
        # if it's consecutive to the job to configure,
        # and if its L2A product is available
        if (
            last_l2a_valid_job
            and FscRlieJobUtil.are_jobs_consecutives(last_l2a_valid_job, job)
            and FscRlieJobUtil.l2a_product_exists(last_l2a_valid_job)
            ):
            # Conditions verified -> configure job to run it with MAJA 'nominal' mode
            job.maja_mode = MajaMode.nominal
            job.job_id_for_last_valid_l2a = last_l2a_valid_job.id
            job.l2a_path_in = last_l2a_valid_job.l2a_path_out
            job.n_jobs_run_since_last_init = last_l2a_valid_job.n_jobs_run_since_last_init + 1
            job.n_l2a_produced_since_last_init = last_l2a_valid_job.n_l2a_produced_since_last_init

        else:
            # Try to retreive jobs that could be used to run a "backward" mode
            backward_required_jobs = FscRlieJobUtil.is_backward_available(
                job, fsc_rlie_job_class(), logger_func)

            # Check if enough data are available to run a "backward"
            if backward_required_jobs and activate_backward_reprocessing:
                job.maja_mode = MajaMode.backward
                job.l1c_id_list=";".join([job.l1c_id for job in backward_required_jobs])
                job.l1c_path_list=";".join([job.l1c_path for job in backward_required_jobs])
                job.n_jobs_run_since_last_init = len(backward_required_jobs)
                job.n_l2a_produced_since_last_init = len(backward_required_jobs)

            # If the database is empty we want to start filling it with a "Backward" job,
            # so we wait to have enough jobs stored in it to do so.
            elif not last_l2a_valid_job and activate_backward_reprocessing:
                return "Waiting to perform backward initialization"

            # Else run in "init" mode, restarting counters
            else:
                job.maja_mode = MajaMode.init
                job.n_jobs_run_since_last_init = 0
                job.n_l2a_produced_since_last_init = 0
                job.backward_reprocessing_run = False

        # If option is activated and enough jobs have been run to reprocess
        #  an old job launched with "init" mode, reprocess it in "backward" mode
        if (
            activate_backward_reprocessing
            and isinstance(job.n_l2a_produced_since_last_init, int)
            and (
                job.n_l2a_produced_since_last_init
                >= maja_backward_required_job_number
                )
            ):
            FscRlieJobUtil.reprocess_with_backward(job, fsc_rlie_job_class, logger_func)


    @staticmethod
    def are_jobs_consecutives(previous_job, job):
        '''
        Check if two jobs are consecutives, if their measurement dates
        difference is inferior to a threshold value.


        :param previous_job: job instance.
        :param job: job instance.
        '''

        # Compute the difference between the two jobs measurement dates
        if previous_job.measurement_date < job.measurement_date:
            measurement_date_difference = job.measurement_date - previous_job.measurement_date
        else:
            measurement_date_difference = previous_job.measurement_date - job.measurement_date

        # Maximum time range (in days) between two jobs measurement dates,
        #  to run MAJA with NOMINAL mode. Expressed as 'timedelta'.
        maja_consecutive_jobs_threshold_value = timedelta(days=SystemPrameters().get(
            temp_logger.debug).maja_consecutive_jobs_threshold_value)

        # Check if the difference is inferior to the threshold value
        return measurement_date_difference <= maja_consecutive_jobs_threshold_value


    @staticmethod
    def is_backward_available(job, fsc_rlie_job_object, logger_func=None):
        '''
        Check if enough jobs have been processed to be able to process selected
        job with BACKWARD mode.
        Note that the job we are checking the BACKWARD availability for is included
        in the list of jobs that are required to run in BACKWARD mode.

        :param job: job instance for which we want to check if Backward is available.
        :param fsc_rlie_job_object: FscRlieJob().
        :param logger_func: logger instance.
        '''

        # Number of job data (L1C) required by MAJA to be able to run a
        #  BACKWARD process.
        maja_backward_required_job_number = SystemPrameters().get(
            temp_logger.debug).maja_backward_required_job_number

        # Call stored procedure to retreive closest jobs, with measurement
        #  date more recent than the current job, and focusing on the same tile
        init_following_jobs = StoredProcedure.fsc_rlie_jobs_following_measurement_with_tile_id(
            job.tile_id,
            job.measurement_date.strftime('%Y-%m-%dT%H:%M:%S'),
            job.l1c_esa_creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
            results_limit=1000, # arbitrary nuber to ensure that we get all the degraded jobs
            fsc_rlie_job_object=fsc_rlie_job_object,
            logger_func=logger_func.debug
            )

        # Keep only the jobs with degraded quality, which generated a L2A,
        # and which did not get reprocessed yet.
        backward_required_jobs = []
        for init_following_job in init_following_jobs:
            if (init_following_job.l2a_status == L2aStatus.generated.name
                and init_following_job.fsc_path is not None
                and not init_following_job.backward_reprocessing_run):

                quality_flag = init_following_job.fsc_path.split('_')[-1]
                if quality_flag == '0':
                    backward_required_jobs.append(init_following_job)

        # There should be 7 degraded jobs for each backward, so if the list
        # is longer, as the jobs are ordered with ascending measurement date,
        # we only keep the 7 first jobs.
        if len(backward_required_jobs) > 7:
            backward_required_jobs = backward_required_jobs[:7]

        if len(backward_required_jobs) == (
            maja_backward_required_job_number - 1
        ):
            # Sort jobs with ascending measurement dates
            backward_required_jobs.sort(key=lambda job: job.measurement_date)

            # List of jobs that should be consecutive for backward to run,
            # the current job is included in this list
            backward_required_jobs = [job] + backward_required_jobs

            for i in range(len(backward_required_jobs)-1):
                if not FscRlieJobUtil.are_jobs_consecutives(
                    backward_required_jobs[i],
                    backward_required_jobs[i+1]
                ):
                    return False

            return backward_required_jobs

        else:
            return False


    @staticmethod
    def l2a_product_exists(job):
        '''
        Check if a job's L2A product is still stored in the bucket.

        :param job: job instance for which we want to check if the L2A is available.
        '''

        # Check job's L2A status
        return job.l2a_status != L2aStatus.deleted


    @staticmethod
    def reprocess_with_backward(job, fsc_rlie_job_class, logger_func=None):
        '''
        Create a new job to reprocess the last "init" job  with MAJA BACKWARD mode,
        taking as input the L1C products of jobs processed after this one,
        to produce a new L2A product with an improved quality.

        :param job: job instance.
        :param fsc_rlie_job_class: FscRlieJob.
        :param logger_func: logger instance.
        '''

        # Retreive last job processed with MAJA 'init' mode which wasn't
        # already reprocessed in Backward.
        init_job = StoredProcedure.fsc_rlie_job_last_init_with_tile_id_no_backward(
            job.tile_id,
            job.measurement_date.strftime('%Y-%m-%dT%H:%M:%S'),
            fsc_rlie_job_class(),
            logger_func.debug
            )

        # The request response is a list -> return the first and unique element
        #  if it exists, else return None
        if isinstance(init_job, list) and len(init_job) > 0:
            init_job = init_job[0]
        else:
            init_job = None

        # Only run backward on init jobs which have generated a L2A product
        if init_job and init_job.l2a_status == L2aStatus.generated.name:
            # Retreive jobs required to run a "backward"
            backward_required_jobs = FscRlieJobUtil.is_backward_available(
                init_job,
                fsc_rlie_job_class(),
                logger_func
                )

            # "Backward" seems not available as we couldn't retreive enough jobs
            #  to process it
            if not backward_required_jobs:
                logger_func.error("Attempted to reprocess init job '%s' while not "\
                    "enough data are available !" %init_job.id)
                return 1

            backward_job = fsc_rlie_job_class(
                tile_id=init_job.tile_id,
                l1c_id=init_job.l1c_id,
                l1c_id_list=";".join([job.l1c_id for job in backward_required_jobs]),
                l1c_path=init_job.l1c_path,
                l1c_path_list=";".join([job.l1c_path for job in backward_required_jobs]),
                l1c_cloud_cover=init_job.l1c_cloud_cover,
                l1c_snow_cover=init_job.l1c_snow_cover,
                n_jobs_run_since_last_init=len(backward_required_jobs),
                n_l2a_produced_since_last_init=len(backward_required_jobs),
                measurement_date=init_job.measurement_date,
                l1c_esa_creation_date=init_job.l1c_esa_creation_date,
                l1c_dias_publication_date=init_job.l1c_dias_publication_date,
                maja_mode="backward",
                reprocessing_context="backward"
            )

            # Always a reprocessing context
            backward_job.nrt = False

            # Set L2A product status
            backward_job.l2a_status = L2aStatus.pending

            # Set the log level used for this next job execution.
            backward_job.next_log_level = FscRlieJobUtil.__FSC_RLIE_LOG_LEVEL

            # Insert the job (fsc_rlie + parent job) into the database
            backward_job.post(post_foreign=True, logger_func=logger_func.debug)

            # Set the job status to initialized
            backward_job.post_new_status_change(JobStatus.initialized)

            # Update init_job to notify that backward job has been run, and that
            # it's not the reference anymore for its L1C.
            init_job.backward_reprocessing_run = True
            init_job.l1c_reference_job = False
            init_job.patch(patch_foreign=True, logger_func=logger_func.debug)

            # Create new jobs to reprocess the jobs with a degraded quality.
            for degraded_job in backward_required_jobs[1:]:
                nominal_job = fsc_rlie_job_class(
                    tile_id=degraded_job.tile_id,
                    l1c_id=degraded_job.l1c_id,
                    l1c_path=degraded_job.l1c_path,
                    l1c_cloud_cover=degraded_job.l1c_cloud_cover,
                    l1c_snow_cover=degraded_job.l1c_snow_cover,
                    measurement_date=degraded_job.measurement_date,
                    l1c_esa_creation_date=degraded_job.l1c_esa_creation_date,
                    l1c_dias_publication_date=degraded_job.l1c_dias_publication_date,
                    reprocessing_context="backward"
                )

                # Always a reprocessing context
                nominal_job.nrt = False

                # Set L2A product status
                nominal_job.l2a_status = L2aStatus.pending

                # Set the log level used for this next job execution.
                nominal_job.next_log_level = FscRlieJobUtil.__FSC_RLIE_LOG_LEVEL

                # Insert the job (fsc_rlie + parent job) into the database
                nominal_job.post(post_foreign=True, logger_func=logger_func.debug)

                # Set the job status to initialized
                nominal_job.post_new_status_change(JobStatus.initialized)

                # Update init_job to notify that backward job has been run, and that
                # it's not the reference anymore for its L1C.
                degraded_job.backward_reprocessing_run = True
                degraded_job.l1c_reference_job = False
                degraded_job.patch(patch_foreign=True, logger_func=logger_func.debug)


    @staticmethod
    def split_bucket_and_object_from_path(bucket_and_object_path: str, prefix: bool = False):
        '''
        Converts "/bucket/path/to/some/object.txt" into
        ("bucket", "path/to/some/object.txt"),
        or "/prefix/bucket/path/to/some/object.txt" into
        ("bucket", "path/to/some/object.txt")
        Works also for "bucket/path/to/some/object.txt" (whithout a leading '/').

        :param bucket_and_object_path: string, in which is stored a path on a bucket.
        :param prefix: boolean, notifying if a prefix is present in the path before
            the bucket name.
        '''

        # Remove any leading/tailing '/'
        bucket_and_object_path = bucket_and_object_path.strip('/')

        if prefix:
            bucket = bucket_and_object_path.split('/', 2)[1]
            object_path = bucket_and_object_path.split('/', 2)[2]
        else:
            bucket = bucket_and_object_path.split('/', 1)[0]
            object_path = bucket_and_object_path.split('/', 1)[1]
        return (bucket, object_path)


    @staticmethod
    def build_path_from_bucket_and_object(bucket: str, object_path: str, prefix: str = ''):
        '''
        Converts ("bucket", "path/to/some/object.txt") into
        "/bucket/path/to/some/object.txt",
        or ("bucket", "path/to/some/object.txt", "prefix") into
        "/prefix/bucket/path/to/some/object.txt".

        :param bucket: string, name of the bucket.
        :param bucket_and_object_path: string, path leading to an object in the bucket.
        :param prefix: string, prefix to add before the bucket name.
        '''

        # Remove any leading/tailing '/'
        bucket = bucket.strip('/')
        object_path = object_path.strip('/')
        prefix = prefix.strip('/')

        # Join strings and add a leading '/'
        if len(prefix) > 0:
            bucket_and_object_path = '/'.join(['', prefix, bucket, object_path])
        else:
            bucket_and_object_path = '/'.join(['', bucket, object_path])

        return bucket_and_object_path


    @staticmethod
    def set_product_bucket_paths(job, product_type: str, product_name: str, logger_func=None):
        '''
        Set job FSc and RLIE bucket path based on the template.

        :param job: job instance.
        :param product_type: string, product type name.
        :param product_name: string, name of the generated product.
        :param logger_func: logger instance.
        '''

        # Product template bucket path
        product_key_prefix_template = (
            "CLMS/Pan-European/High_Resolution_Layers/{}/{}/{}/{}/{}/{}"
            )

        # Bucket name
        bucket_name = "HRSI"
        # Bucket prefix
        bucket_prefix = "eodata"

        # FSC & RLIE software names
        if product_type.upper() == "FSC":
            software_name = "Snow"
        else:
            software_name = "Ice"

        # Set job key prefix path attribute
        key_prefix = product_key_prefix_template.format(
            software_name,
            product_type,
            job.measurement_date.year,
            ("0%s" %(job.measurement_date.month)
                if job.measurement_date.month < 10
                else job.measurement_date.month),
            ("0%s" %(job.measurement_date.day)
                if job.measurement_date.day < 10
                else job.measurement_date.day),
            product_name
        )

        # Call function to build path from bucket and key_prefix
        if product_type.upper() == "FSC":
            job.fsc_path = FscRlieJobUtil.build_path_from_bucket_and_object(
                bucket_name,
                key_prefix,
                prefix=bucket_prefix
            )
        else:
            job.rlie_path = FscRlieJobUtil.build_path_from_bucket_and_object(
                bucket_name,
                key_prefix,
                prefix=bucket_prefix
            )


    @staticmethod
    def compute_product_quality_flag(job, logger_func=None):
        '''
        Compute quality flag for FSC/RLIE products.

        :param job: job instance.
        :param logger_func: logger instance.
        '''

        # Number of job data (L1C) required by MAJA to be able to run a
        #  BACKWARD process.
        maja_backward_required_job_number = SystemPrameters().get(
            temp_logger.debug).maja_backward_required_job_number

        # Compute quality flag
        quality_flag = ("0"
            if (
                job.n_l2a_produced_since_last_init
                < maja_backward_required_job_number
            )
            else "1"
        )

        return quality_flag


    @staticmethod
    def get_quicklook_bucket_path(result_bucket_path: str):
        '''
        Compute the path leading to a product quicklook on the bucket.

        :param result_bucket_path: string, path leading to the product on the bucket.
        '''

        quicklook_folder = "Preview"
        quicklook_bucket_path = os.path.join(quicklook_folder, result_bucket_path)
        return quicklook_bucket_path


    @staticmethod
    def statistics(jobs, fsc_rlie_job_class, logger):
        '''
        Statistics on returned jobs (for debugging).

        :param jobs: list of jobs on which we want to display statistics.
        :param fsc_rlie_job_class: FscRlieJob class.
        :param logger_func: Logger instance.
        '''

        if not jobs:
            return

        msg = ''
        original_count = len(jobs)

        # Keep only the EEA39 tiles
        jobs = [j for j in jobs if j.tile_id in fsc_rlie_job_class.__JOBS_GEOMETRY]
        msg += 'EEA39 tiles: %0.2f%%\n' % (100 * len(jobs) / original_count)

        # Count the number of jobs for each tile.
        tile_count = {
            key: len([j for j in jobs if j.tile_id == key])
            for key in fsc_rlie_job_class.__JOBS_GEOMETRY}

        # Group them by value
        counts = {}
        for key, value in sorted(tile_count.items()):
            counts.setdefault(value, []).append(key)
        for key, value in sorted(counts.items()):
            msg += 'Tiles with %03d products: %s\n' % (key, ", ".join(value))

        logger.info("Statistics:\n%s" % msg)



    @staticmethod
    def get_unique_jobs(jobs, last_inserted_job, fsc_rlie_job_class, internal_database_parallel_request, logger):
        '''
        Keep only the jobs that do not already exist in the database.

        :param jobs: list of FSC RLIE jobs to test.
        :param last_inserted_job: last FSC RLIE job to have been inserted in database.
        :param fsc_rlie_job_class: FscRlieJob class.
        :param internal_database_parallel_request: number of requests to the
            database that can be done in parallel.
        :param logger_func: Logger instance.
        '''

        if not jobs:
            return []

        # Send Get requests to the database to find the existing jobs,
        # based on their tile ID and measurement_date.
        # Maybe we should use a more complex condition, e.g. ID + publication date.
        def is_same(job1, job2, strictly=False):
            if (job1 is None) and (job2 is None):
                return True
            elif (job1 is None) or (job2 is None):
                return False
            else:
                if strictly:
                    return job1.get_input_product_id() == job2.get_input_product_id()
                else:
                    return (
                        job1.tile_id == job2.tile_id
                        and (job1.measurement_date.replace(tzinfo=None) 
                            == job2.measurement_date.replace(tzinfo=None))
                    )

        def contains(jobs, job1, strictly=False):
            return any(is_same(job1, job2, strictly) for job2 in jobs)


        # We split the ID list into smaller lists so the sent URL is not too long.
        jobs_split = []
        jobs_sub = []
        for job in jobs:
            if (len(jobs_sub) == 0) or (len(jobs_sub) >= 20):
                jobs_sub = []
                jobs_split.append([jobs_sub, logger.debug])
            jobs_sub.append(job)

        # Run the multithreaded requests and wait for finish.
        pool = Pool(internal_database_parallel_request)
        existing_jobs = pool.starmap(fsc_rlie_job_class.get_existing_input_products, jobs_split)
        pool.close()
        pool.join()

        # Flatten the list of lists
        existing_jobs = list(itertools.chain.from_iterable(existing_jobs))

        # Only keep the jobs that do not already exist in the database
        # or that are re-published within 24h after their measurement.
        valid_jobs = []
        for job in jobs:
            if (
                not contains(existing_jobs, job, strictly=False)
                and not contains(valid_jobs, job, strictly=True)
            ):
                valid_jobs.append(job)

            # Ensure the input product has been published within 24h after its measurement
            elif (
                not contains(existing_jobs, job, strictly=True)
                and (job.get_input_product_dias_publication_date().replace(tzinfo=None) 
                - job.measurement_date.replace(tzinfo=None)
                ) <= fsc_rlie_job_class.DUPLICATE_INPUT_PRODUCT_VALID_TIME
            ):
                for existing_job in existing_jobs:
                    # Ensure the input product has been re-published
                    if (
                        is_same(job, existing_job, strictly=False)
                        and (
                            job.get_input_product_esa_creation_date().replace(tzinfo=None) 
                            != existing_job.get_input_product_esa_creation_date().replace(tzinfo=None)
                        )
                    ):
                        # Only add job to the list if it's not present in it already
                        if not contains(valid_jobs, job, strictly=True):
                            valid_jobs.append(job)

                        # Update old job to notify that it's not the reference for
                        # the given input product anymore, the one we are ceating
                        # will now be the reference.
                        existing_job.set_input_product_reference_job(False)
                        existing_job.patch(patch_foreign=True, logger_func=logger.debug)

        jobs = valid_jobs

        # Find the duplicate jobs and print a warning message.
        # Do not print about the last inserted job. We know that it already exists.
        # It appears here because the date_max of the last search = the date_min of
        # the new search.
        duplicate_jobs = [
            job
            for job in jobs
            if contains(existing_jobs, job) and not is_same(job, last_inserted_job)]
        if duplicate_jobs:
            logger.warning(
                '%s jobs with the following input product IDs '\
                'already exist in the database:\n - %s' %
                (fsc_rlie_job_class.JOB_NAME, '\n - '.join(
                    [job.get_input_product_id() for job in duplicate_jobs])))

        return jobs  


    @staticmethod
    def filter_reprocessed_input_products(jobs, fsc_rlie_processing_start_date):
        '''
        If input products are published on DIAS recently, but their measurement date
        is older than the operational system processing start date, it
        means that it's part of an ESA reprocessing campaign, and we shouldn't
        process them in the NRT flow.
        Thus, we create jobs with status 'cancelled' for these input products, to keep
        track of them and not process them.

        :param jobs: list of FSC RLIE jobs that we filter.
        :param fsc_rlie_processing_start_date: datetime object refering to the date
            at which the operational system started to process S2 data.
        '''

        if not jobs:
            return [], []

        # List of jobs which will be processed
        valid_jobs = []
        # List of codated jobs which won't be processed
        reprocessed_jobs = []

        for job in jobs:
            if (
                job.measurement_date.replace(tzinfo=None)
                < fsc_rlie_processing_start_date
            ):
                reprocessed_jobs.append(job)
            else:
                valid_jobs.append(job)

        return valid_jobs, reprocessed_jobs


    @staticmethod
    def update_jobs_with_esa_info(jobs, logger, status_update=True):
        '''
        Update the jobs with the information from the ESA SciHub, in particular
        with the "esa publication date" to keep track of the job timeliness.

        :param jobs: list of jobs to be updated with ESA info.
        :param status_update: boolean, to determine if jobs status should be
            updated if no info on ESA can be found on the jobs.
        '''

        # Retrieve the ESA publication date for a set of jobs
        jobs_ids = [job.get_input_product_id() for job in jobs]
        jobs_esa_publication_date_dictionary = EsaUtil().request(jobs_ids)

        # List of missing products
        missing = []

        # Update the ESA date for each job
        for job in jobs:
            try:
                job.set_input_product_esa_publication_date(
                    jobs_esa_publication_date_dictionary[job.get_input_product_id()])
            except Exception:
                missing.append(job.get_input_product_id())
                # Notify that an error occured through jobs status
                if status_update:
                    job.error_raised = True
                    job.post_new_status_change(
                        JobStatus.external_error, 
                        error_subtype="ESA SciHub Error",
                        error_message="Products are missing from the ESA request."    
                    )
                    job.post_new_status_change(JobStatus.error_checked)
                    job.post_new_status_change(JobStatus.initialized)

        if missing:
            logger.warning(
                "The following products are missing from the ESA request: \n - %s" % (
                    '\n - '.join(missing)))
        return jobs


    @staticmethod
    def update_priority(jobs):
        '''
        Update job priority levels.

        Each job is attributed a priority level that affects its delay. The priority levels are NRT, DELAYED and REPROCESSING.
        In the Near Real-Time context:

        * The NRT level is the highest priority with timeliness criticality:
          * Input product publication in the ESA hub after acquisition < 24 hours AND
          * Input product publication in the DIAS after ESA < 3 hours
        * The DELAYED level is used for late Input product publication:
          * Input product publication in the ESA hub after acquisition > 24 hours OR
          * Input product publication in the DIAS after ESA > 3 hours
        The REPROCESSING level is used in the archive reprocessing, and aggregated products contexts.

        :param jobs: list of jobs for which we want to update the priority.
        '''

        for job in jobs:

            # Jobs in a Near Real-Time (NRT) context
            if job.nrt:

                # All the dates are defined
                if (
                    job.measurement_date and
                    job.get_input_product_esa_publication_date() and
                    job.get_input_product_dias_publication_date()
                ):
                    # Remove the time zone (expect UTC)
                    measurement = job.measurement_date.replace(tzinfo=None)
                    esa = job.get_input_product_esa_publication_date().replace(tzinfo=None)
                    dias = job.get_input_product_dias_publication_date().replace(tzinfo=None)

                    if (
                        # Time between measurement and ESA is < 24hrs
                        ((esa - measurement) < timedelta(hours=24)) and

                            # Time between ESA and the DIAS is < 3hrs
                            ((dias - esa) < timedelta(hours=3))):

                        # NRT level
                        job.priority = JobPriority.nrt

                # Time between measurement and ESA is > 24hrs or
                # Time between ESA and the DIAS is > 3hrs
                # -> delayed level
                if not job.priority:
                    job.priority = JobPriority.delayed

            # Archive reprocessing, and aggregated products: reprocessing level
            else:
                job.priority = JobPriority.reprocessing

        return jobs


    @staticmethod
    def fail_if_inconsistent_status_list():
        '''
        Ensure that all the existing job status are covered
        by the two lists defined at the FscRlieJobUtil class level.
        '''

        job_status_list = (
            FscRlieJobUtil.INITIALIZED_STATUS
            + FscRlieJobUtil.RUNNING_STATUS
            + FscRlieJobUtil.PROCESSING_COMPLETED_STATUS
        )

        missing_status = []
        status_to_remove = []

        for status in JobStatus:
            if status not in job_status_list:
                missing_status.append(status.name)
        for status in job_status_list:
            if status not in JobStatus:
                status_to_remove.append(status.name)

        if missing_status:
            print("Error : lists of status in FscRlieJobUtil class are not up to date! "
            "Missing %s status!" %missing_status)
            sys.exit(1)
        if status_to_remove:
            print("Error : lists of status in FscRlieJobUtil class are not up to date! "
            "Status %s should be removed!" %status_to_remove)
            sys.exit(1)
        if len(job_status_list) != len(JobStatus):
            status_to_remove = [
                status.name
                for status
                in job_status_list
                if (job_status_list.count(status) > 1)
                ]
            print("Error : duplicated status %s found in FscRlieJobUtil status list!"
                  %status_to_remove)
            sys.exit(1)

# Static call: check job status lists consistency
#  and read the configuration file
FscRlieJobUtil.fail_if_inconsistent_status_list()
FscRlieJobUtil.read_config_file()
