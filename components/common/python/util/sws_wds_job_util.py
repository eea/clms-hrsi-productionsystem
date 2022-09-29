import sys
import os
import logging
import itertools
from datetime import timedelta
from yaml import safe_load as yaml_load

from ..database.model.job.job_status import JobStatus
from ..database.model.job.job_priority import JobPriority
from ..database.model.job.system_parameters import SystemPrameters
from ..database.model.job.sws_wds_assembly_status import AssemblyStatus
from ..database.rest.stored_procedure import StoredProcedure
from ..util.log_util import temp_logger
from ..util.esa_util import EsaUtil
from .resource_util import ResourceUtil


class SwsWdsJobUtil(object):
    '''utility functions related to SWS WDS scientific application'''

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

    # The initial log level for the SWS/WDS jobs can be modified later by the
    #  operator for e.g. debugging jobs.
    __SWS_WDS_LOG_LEVEL = None

    @staticmethod
    def read_config_file():
        '''Read the configuration file.'''
        with open(SwsWdsJobUtil.__CONFIG_PATH, 'r') as stream:
            contents = yaml_load(stream)
            SwsWdsJobUtil.__SWS_WDS_LOG_LEVEL = (
                logging.getLevelName(contents['job_log_level']))

    @staticmethod
    def get_tiles_mountains_config():
        '''
        Read tile_ids of mountains for SWS processing
        '''

        # read the tiles suitable for the SWS product
        mountain_tiles_file = ResourceUtil.for_component('job_creation/geometry/eea39_aoi/tiles_mountains.txt')
        with open(mountain_tiles_file) as fd:
            mountain_tiles = fd.read().splitlines()

        return mountain_tiles

    @staticmethod
    def get_tiles_tracks_config():
        '''
        Read the tracks per tile, where we have auxiliary data
        '''

        # read the tiles and tracks, where we have the reference and auxiliary files for the SWS/WDS products
        tiles_tracks_file = ResourceUtil.for_component('job_creation/geometry/eea39_aoi/tiles_tracks_sws_wds_process.yml')
        with open(tiles_tracks_file) as fd:
            tiles_tracks = yaml_load(fd)

        return tiles_tracks

    @staticmethod
    def find_previous_temporal_job_no_s1_restriction(job, sws_wds_job_object, allow_codated_jobs=False, logger_func=None):
        '''
        Retreive master job, with measurement date inferior to the one passed
        in argument, focusing on the same tile, and which didn't fail to
        generate an S1 Assebly product yet. (it can be a job being configured/run and
        which didn't attempt to produce any S1 Ass yet, or a job which already
        successfully generated one)

        :param job: job instance for which we want to find the master job.
        :param sws_wds_job_object: SwsWdsJob().
        :param allow_codated_jobs: boolean, to notify if codated jobs can be returned.
        :param logger_func: logger instance.
        '''

        # Call stored procedure to retreive the most recent job, with a measurement
        #  date inferior to the current job, focusing on the same tile, and which
        #  didn't fail to produce a L2A yet
        previous_job = StoredProcedure.get_last_job_with_usable_s1ass(
            assembly_id=job.assembly_id,
            allow_codated_jobs=allow_codated_jobs,
            sws_wds_job_object=sws_wds_job_object,
            logger_func=logger_func.debug
        )

        # The request response is a list -> return the first and unique element
        #  if it exists, else return None
        if isinstance(previous_job, list) and len(previous_job) > 0:
            previous_job = previous_job[0]
        else:
            previous_job = None

        return previous_job

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
        Set job SWS and WDS bucket path based on the template.

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

        # SWS & WDS software names
        if product_type.upper() == "SWS":
            software_name = "WetSnow"
        else:
            software_name = "WetDrySnow"

        # Set job key prefix path attribute
        key_prefix = product_key_prefix_template.format(
            software_name,
            product_type,
            job.measurement_date.year,
            "%02d" % job.measurement_date.month,
            "%02d" % job.measurement_date.day,
            product_name
        )

        # Call function to build path from bucket and key_prefix
        if product_type.upper() == "SWS":
            job.sws_path = SwsWdsJobUtil.build_path_from_bucket_and_object(
                bucket_name,
                key_prefix,
                prefix=bucket_prefix
            )
        else:
            job.wds_path = SwsWdsJobUtil.build_path_from_bucket_and_object(
                bucket_name,
                key_prefix,
                prefix=bucket_prefix
            )

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
    def statistics(jobs, job_class, logger):
        '''
        Statistics on returned jobs (for debugging).

        :param jobs: list of jobs on which we want to display statistics.
        :param job_class: instance of job class.
        :param logger: Logger instance.
        '''

        if not jobs:
            return

        msg = ''
        original_count = len(jobs)

        # Keep only the EEA39 tiles
        jobs = [j for j in jobs if j.tile_id in job_class.__JOBS_GEOMETRY]
        msg += 'EEA39 tiles: %0.2f%%\n' % (100 * len(jobs) / original_count)

        # Count the number of jobs for each tile.
        tile_count = {
            key: len([j for j in jobs if j.tile_id == key])
            for key in job_class.__JOBS_GEOMETRY}

        # Group them by value
        counts = {}
        for key, value in sorted(tile_count.items()):
            counts.setdefault(value, []).append(key)
        for key, value in sorted(counts.items()):
            msg += 'Tiles with %03d products: %s\n' % (key, ", ".join(value))

        logger.info("Statistics:\n%s" % msg)

    @staticmethod
    def filter_reprocessed_input_products(jobs, sws_wds_processing_start_date):
        '''
        If input products are published on DIAS recently, but their measurement date
        is older than the operational system processing start date, it
        means that it's part of an ESA reprocessing campaign, and we shouldn't
        process them in the NRT flow.
        Thus, we create jobs with status 'cancelled' for these input products, to keep
        track of them and not process them.

        :param jobs: list of SWS WDS jobs that we filter.
        :param sws_wds_processing_start_date: datetime object refering to the date
            at which the operational system started to process S1 data.
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
                < sws_wds_processing_start_date
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
        :param logger: logger instance.
        :param status_update: boolean, to determine if jobs status should be
            updated if no info on ESA can be found on the jobs.
        '''

        # Retrieve the ESA publication date for a set of jobs
        jobs_ids = []
        for job in jobs:
            ids = job.get_input_product_id()
            if type(ids) == list:
                jobs_ids.extend(ids)
            else:
                jobs_ids.append(ids)
        jobs_esa_publication_date_dictionary = EsaUtil().request(jobs_ids)

        # List of missing products
        missing = []

        # Update the ESA date for each job
        for job in jobs:
            try:
                ids = job.get_input_product_id()
                if type(ids) == list:
                    d = max([jobs_esa_publication_date_dictionary[i] for i in ids])
                    job.set_input_product_esa_publication_date(d)
                else:
                    job.set_input_product_esa_publication_date(jobs_esa_publication_date_dictionary[ids])
            except Exception:
                ids = job.get_input_product_id()
                if type(ids) == list:
                    missing.extend(ids)
                else:
                    missing.append(ids)
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
    def update_jobs_for_assembly_master(jobs, job_class, logger):
        '''
        :param jobs: list of jobs to set the assembly_master_job_id.
        :param job_class: instance of job class.
        :param logger: Logger instance.
        '''

        date_min = min([j.measurement_date for j in jobs]) - timedelta(seconds=80)  # start_time of product is upto 2 slices different
        date_max = max([j.measurement_date for j in jobs]) + timedelta(seconds=80)

        jobs_all = StoredProcedure.get_jobs_within_measurement_date(
            job_class,
            'measurement_date',
            date_min,
            date_max,
            logger_func=logger.debug)

        # get all valid master jobs
        master_job_assemble_id_list = {j.assembly_id: j.id for j in jobs_all if (
            j.assembly_master_job_id == 0 and
            AssemblyStatus[j.assembly_status].value < AssemblyStatus.generation_aborted.value)}

        for job in jobs:
            # find assembly master job
            if job.assembly_id in master_job_assemble_id_list:
                job_of_assembleId = master_job_assemble_id_list[job.assembly_id]
            else:
                master_job_assemble_id_list[job.assembly_id] = job.id
                job_of_assembleId = 0
            job.assembly_master_job_id = job_of_assembleId     # 0 means this job is master
            # logger.debug("assembly_master_job_id: %s %s %s" % (str(job.id), str(job.name), str(job.assembly_master_job_id)))

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
        by the two lists defined at the SwsJobUtil class level.
        '''

        job_status_list = (
            SwsWdsJobUtil.INITIALIZED_STATUS
            + SwsWdsJobUtil.RUNNING_STATUS
            + SwsWdsJobUtil.PROCESSING_COMPLETED_STATUS
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
            print("Error : lists of status in SwsJobUtil class are not up to date! "
                  "Missing %s status!" % missing_status)
            sys.exit(1)
        if status_to_remove:
            print("Error : lists of status in SwsJobUtil class are not up to date! "
                  "Status %s should be removed!" % status_to_remove)
            sys.exit(1)
        if len(job_status_list) != len(JobStatus):
            status_to_remove = [
                status.name
                for status
                in job_status_list
                if (job_status_list.count(status) > 1)
            ]
            print("Error : duplicated status %s found in SwsJobUtil status list!"
                  % status_to_remove)
            sys.exit(1)


# Static call: check job status lists consistency
#  and read the configuration file
SwsWdsJobUtil.fail_if_inconsistent_status_list()
SwsWdsJobUtil.read_config_file()
