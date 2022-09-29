import sys
import os
import logging
import itertools
from datetime import timedelta
from multiprocessing.dummy import Pool
import yaml
import datetime

from ..database.model.job.job_status import JobStatus
from ..database.model.job.job_priority import JobPriority
from ..database.rest.stored_procedure import StoredProcedure
from ..database.model.job.system_parameters import SystemPrameters
from ..util.log_util import temp_logger
from ..util.esa_util import EsaUtil
from.resource_util import ResourceUtil
from ..util.datetime_util import DatetimeUtil


class GfscJobUtil(object):
    '''utility functions related to GFSC scientific application'''

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
    __GFSC_RLIE_LOG_LEVEL = None


    @staticmethod
    def read_config_file():
        '''Read the configuration file.'''
        with open(GfscJobUtil.__CONFIG_PATH, 'r') as stream:
            contents = yaml.safe_load(stream)
            GfscJobUtil.__GFSC_RLIE_LOG_LEVEL = (
                logging.getLevelName(contents['job_log_level']))
                
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
        Set job GFSC bucket path based on the template.
        
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

        software_name = "Snow"

        # Set job key prefix path attribute
        measurement_date = job.product_date if product_type is 'GFSC' else job.measurement_date
        key_prefix = product_key_prefix_template.format(
            software_name,
            product_type,
            measurement_date.year,
            ("0%s" %(measurement_date.month) 
                if measurement_date.month < 10 
                else measurement_date.month),
            ("0%s" %(measurement_date.day) 
                if measurement_date.day < 10 
                else measurement_date.day),
            product_name
        )

        # Call function to build path from bucket and key_prefix
        job.gfsc_path = GfscJobUtil.build_path_from_bucket_and_object(
            bucket_name, 
            key_prefix,
            prefix=bucket_prefix
        )

    @staticmethod
    def get_input_product_type(product_id):
        return product_id.split('_')[0].lower()

    @staticmethod
    def get_input_product_date(product_id):
        productDate = product_id.split('_')[1]
        if '-' in productDate:
            productDate = productDate.split('-')[0]
        if 'T' in productDate:
            productDate = productDate.split('T')[0]
        productDate = datetime.datetime.strptime(productDate,"%Y%m%d")
        productDate = datetime.date(productDate.year,productDate.month,productDate.day)
        return productDate

    @staticmethod
    def get_input_product_measurement_date(product_id):
        productDate = product_id.split('_')[1]
        if '-' in productDate:
            productDate = productDate.split('-')[0]
            productDate = datetime.datetime.strptime(productDate,"%Y%m%d")
        if 'T' in productDate:
            productDate = productDate.split('T')[0]
            productDate = datetime.datetime.strptime(productDate,"%Y%m%dT%H%M%S")
        return productDate

    @staticmethod
    def get_input_product_mode(product_id):
        productType = product_id.split('_')[0]
        if productType == 'GFSC':
            return False
        return product_id.split('_')[5]

    @staticmethod
    def get_input_product_mode_toggled_id(product_id):
        productType = product_id.split('_')[0]
        if productType == 'GFSC':
            return False
        product_id = product_id.split('_')
        if product_id[-1] == '0':
            product_id[-1] = '1'
        elif product_id[-1] == '1':
            product_id[-1] = '0'
        else:
            return False
        return '_'.join(product_id)

    @staticmethod
    def get_product_bucket_path(product_id,fname=None):
        basePath = 'CLMS/Pan-European/High_Resolution_Layers/Snow/'
        if fname == 'thumbnail.png':
            basePath = os.path.join('Preview', basePath)
        productDate = GfscJobUtil.get_input_product_date(product_id)
        productType = product_id.split('_')[0]
        remoteFilePath = os.path.join(basePath,productType,productDate.strftime("%Y"),productDate.strftime("%m"),productDate.strftime("%d"),product_id)
        if fname is not None:
            remoteFilePath = os.path.join(remoteFilePath,fname)
        return remoteFilePath


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
    def statistics(jobs, gfsc_job_class, logger):
        '''
        Statistics on returned jobs (for debugging).
        
        :param jobs: list of jobs on which we want to display statistics.
        :param gfsc_job_class: GfscJob class.
        :param logger_func: Logger instance.
        '''

        if not jobs:
            return

        msg = ''
        original_count = len(jobs)

        # Keep only the EEA39 tiles
        jobs = [j for j in jobs if j.tile_id in gfsc_job_class.__JOBS_GEOMETRY]
        msg += 'EEA39 tiles: %0.2f%%\n' % (100 * len(jobs) / original_count)

        # Count the number of jobs for each tile.
        tile_count = {
            key: len([j for j in jobs if j.tile_id == key])
            for key in gfsc_job_class.__JOBS_GEOMETRY}

        # Group them by value
        counts = {}
        for key, value in sorted(tile_count.items()):
            counts.setdefault(value, []).append(key)
        for key, value in sorted(counts.items()):
            msg += 'Tiles with %03d products: %s\n' % (key, ", ".join(value))

        logger.info("Statistics:\n%s" % msg)

    @staticmethod
    def fail_if_inconsistent_status_list():
        '''
        Ensure that all the existing job status are covered 
        by the two lists defined at the GfscJobUtil class level.
        '''

        job_status_list = (
            GfscJobUtil.INITIALIZED_STATUS
            + GfscJobUtil.RUNNING_STATUS 
            + GfscJobUtil.PROCESSING_COMPLETED_STATUS
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
            print("Error : lists of status in GfscJobUtil class are not up to date! "
            "Missing %s status!" %missing_status)
            sys.exit(1)
        if status_to_remove:
            print("Error : lists of status in GfscJobUtil class are not up to date! "
            "Status %s should be removed!" %status_to_remove)
            sys.exit(1)
        if len(job_status_list) != len(JobStatus):
            status_to_remove = [
                status.name 
                for status 
                in job_status_list 
                if (job_status_list.count(status) > 1)
                ]
            print("Error : duplicated status %s found in GfscJobUtil status list!"
                  %status_to_remove)
            sys.exit(1)

    @staticmethod
    def get_unique_jobs(jobs, last_inserted_job, gfsc_job_class, internal_database_parallel_request, logger):
        '''
        Keep only the jobs that do not already exist in the database.

        :param jobs: list of GFSC jobs to test.
        :param last_inserted_job: last GFSC job to have been inserted in database.
        :param fsc_rlie_job_class: GfscJob class.
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
                    return (
                        job1.triggering_product_id == job2.triggering_product_id
                        and job1.curation_timestamp == job2.curation_timestamp
                    )
                else:
                    return (
                        job1.triggering_product_id == job2.triggering_product_id
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
        existing_jobs = pool.starmap(gfsc_job_class.get_existing_input_products, jobs_split)
        pool.close()
        pool.join()

        # Flatten the list of lists
        existing_jobs = list(itertools.chain.from_iterable(existing_jobs))

        # Only keep the jobs that do not already exist in the database
        valid_jobs = []
        for job in jobs:
            if (
                not contains(existing_jobs, job, strictly=False)
                and not contains(valid_jobs, job, strictly=True)
            ):
                valid_jobs.append(job)
            else:
                logger.warning(
                '%s job with the following triggering product ID '\
                'already exist in the database:\n - %s. Discarding the job.' %
                (gfsc_job_class.JOB_NAME, job.triggering_product_id))

        jobs = valid_jobs

        return jobs

    @staticmethod
    def filter_reprocessed_input_products(jobs, gfsc_processing_start_date):
        '''
        If input products are published on DIAS recently, but their measurement date
        (i.e. product date) is older than the operational system processing start date, it 
        means that it's part of an ESA reprocessing campaign, and we shouldn't
        process them in the NRT flow.
        Thus, we create jobs with status 'cancelled' for these input products, to keep
        track of them and not process them.
        
        :param jobs: list of GFSC jobs that we filter.
        :param gfsc_processing_start_date: datetime object refering to the date 
            at which the operational system started to process data.
        '''

        if not jobs:
            return [], []

        # List of jobs which will be processed
        valid_jobs = []
        # List of codated jobs which won't be processed
        reprocessed_jobs = []

        for job in jobs:
            if (
                job.product_date.replace(tzinfo=None) 
                < gfsc_processing_start_date
            ):
                reprocessed_jobs.append(job)
            else:
                valid_jobs.append(job)

        return valid_jobs, reprocessed_jobs 

# Static call: check job status lists consistency 
#  and read the configuration file
GfscJobUtil.fail_if_inconsistent_status_list()
GfscJobUtil.read_config_file()