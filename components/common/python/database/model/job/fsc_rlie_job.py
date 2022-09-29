import os
import re
import json
import copy
from datetime import datetime, timedelta

from .job_template import JobTemplate
from .l2a_status import L2aStatus
from .job_status import JobStatus
from .maja_mode import MajaMode
from .worker_flavors import WorkerFlavors
from .system_parameters import SystemPrameters
from ....util.datetime_util import DatetimeUtil
from ....util.fsc_rlie_job_util import FscRlieJobUtil
from ....util.eea39_util import Eea39Util
from ....util.sys_util import SysUtil
from ....util.creodias_util import CreodiasUtil
from ....util.exceptions import CsiInternalError
from ...rest.stored_procedure import StoredProcedure


class FscRlieJob(JobTemplate):
    '''
    Description of a job for the generation of one Fractional Snow Cover (FSC)
    and one River and Lake Ice Extent (RLIE) product from a single Sentinel-2 L1C product.

    :param nrt: (boolean) Near Real-Time context ? If false: archive reprocessing.
    :param l1c_id: L1C product ID
    :param l1c_id_list: List of L1C product ID used to reprocess job in Backward.
    :param l1c_cloud_cover: L1C product cloud cover percentage.
    :param l1c_snow_cover: L1C product snow cover percentage.
    :param l1c_path: L1C product path in the DIAS
    :param l1c_path_list: List of L1C product path in the DIAS used to reprocess job in Backward.
    :param l2a_path_in: Input L2A file path in the DIAS, if applicable.
    :param l2a_path_out: Output L2A file path in the DIAS, if applicable.
    :param save_full_l2a: Save the full L2A output (~3GB). Else save only the
    files necessary to the FSC and RLIE processing (~16MB).
    :param job_id_for_last_valid_l2a: id of the job present in database with closest inferior
    measurement date compared with current job which have generated a valid L2A product.
    :param l2a_status: (enum) status of the L2A product.
    :param n_jobs_run_since_last_init: number of jobs processed since
    last job run with MAJA "init" mode (not taking in account late jobs).
    :param n_l2a_produced_since_last_init: number of L2A products processed since
    last job run with MAJA "init" mode (not taking in account late jobs).
    :param dtm_path: DTM path in the DIAS
    :param fsc_infos: (json) information relative to FSC product.
    :param rlie_infos: (json) information relative to RLIE product.
    :param fsc_path: FSC product path in the DIAS
    :param rlie_path: RLIE product path in the DIAS
    :param measurement_date: (datetime) Datatake sensing start time
    :param l1c_sensing_time: (datetime) sensing time from MTD_TL.xml
    :param l1c_esa_creation_date: (datetime) L1C creation date by ESA.
    :param l1c_esa_publication_date: (datetime) L1C publication date in the ESA hub.
    :param l1c_dias_publication_date: (datetime) L1C publication date in the DIAS catalogue.
    :param fsc_completion_date: (datetime) FSC publication date in the bucket.
    :param rlie_completion_date: (datetime) RLIE publication date in the bucket.
    :param fsc_json_publication_date: (datetime) FSC JSON publication date on RabbitMQ endpoint.
    :param rlie_json_publication_date: (datetime) RLIE JSON publication date on RabbitMQ endpoint.
    :param maja_mode: MajaMode
    :param maja_threads: Number of threads for Maja. Overrides the NbThreads value
    in userconf/MAJAUserConfigSystem.xml
    :param maja_other_params: Other Maja options, as command line parameters.
    :param maja_return_code: Return code of Maja execution.
    :param backward_reprocessing_run: (boolean) Notify if a Backward reprocessing should (False)
    or has already (True) be run on this job.
    :param reprocessing_context: (string) Notify the reprocessing context for a non-NRT job
    '''

    # Database table name
    __TABLE_NAME = "fsc_rlie_jobs"

    # Class job name
    JOB_NAME = "FSC/RLIE"

    # Sentinel input product type
    INPUT_PRODUCT_TYPE = "s2"

    # Name of the Nomad job processing the present job type
    NOMAD_JOB_NAME = "si-processing"

    # Worker flavor required for the present job type processing
    WORKER_FLAVOR_NAME = WorkerFlavors.medium.value

    # Name of the products the job will generate during it's processing
    OUTPUT_PRODUCTS_LIST = ["fsc", "rlie"]

    # L2A data bucket's name
    __SIP_DATA_BUCKET = SysUtil.read_env_var("CSI_SIP_DATA_BUCKET")

    # Restrict the orchestrator to a specific tile ID.
    # __TILE_RESTRICTION = ['30TYN', '32TLR', '32TMS', '33WWR', '33WXS', '34WBD', 
    #                       '32TLS', '32TLT', '38SLJ', '38SMJ', '38TMK']
    __TILE_RESTRICTION = None

    # List of all the EEA39 Sentinel-2 tile IDs or filtered accordint __TILE_RESTRICTION.
    __TILE_IDS = Eea39Util.get_tiles(tile_restriction=__TILE_RESTRICTION)

    # EEA39 Area Of Interest (AOI) to request, in WGS84 projection.
    __GEOMETRY = Eea39Util.get_geometry(tile_restriction=__TILE_RESTRICTION)

    # Max number of pages to request (if None: infinite).
    __MAX_REQUESTED_PAGES = None if __TILE_RESTRICTION is None else 20

    # Keep track of the last FSC RLIE job inserted in the database.
    LAST_INSERTED_JOB = None

    # Time after the input product acquisition within which we replace any job
    # already generated from this input product.
    DUPLICATE_INPUT_PRODUCT_VALID_TIME = timedelta(days=1)

    # Name of the stored procedure used to retrieve FscRlieJobs with a given status
    GET_JOBS_WITH_STATUS_PROCEDURE_NAME = "fsc_rlie_jobs_with_last_status"


    def __init__(self, **kwds):
        self.nrt = None
        self.l1c_id = None
        self.l1c_id_list = None
        self.l1c_reference_job = None
        self.l1c_cloud_cover = None
        self.l1c_snow_cover = None
        self.l1c_path = None
        self.l1c_path_list = None
        self.l2a_path_in = None
        self.l2a_path_out = None
        self.save_full_l2a = None
        self.job_id_for_last_valid_l2a = None
        self.l2a_status = None
        self.n_jobs_run_since_last_init = None
        self.n_l2a_produced_since_last_init = None
        self.dtm_path = None
        self.fsc_infos = None
        self.rlie_infos = None
        self.fsc_path = None
        self.rlie_path = None
        self.measurement_date = None
        self.l1c_sensing_time = None
        self.l1c_esa_creation_date = None
        self.l1c_esa_publication_date = None
        self.l1c_dias_publication_date = None
        self.fsc_completion_date = None
        self.rlie_completion_date = None
        self.fsc_json_publication_date = None
        self.rlie_json_publication_date = None
        self.maja_mode = None
        self.maja_threads = None
        self.maja_other_params = None
        self.maja_return_code = None
        self.backward_reprocessing_run = None
        self.reprocessing_context = None

        # Call the parent constructor AFTER all the attributes are initialized with None
        super().__init__(FscRlieJob.__TABLE_NAME)

        # Attribute values given by the caller
        for key, value in kwds.items():
            setattr(self, key, value)


    ################################################################
    # Start of specific methods

    def from_database_value(self, attribute, value):
        '''Parse a string value as it was inserted in the database.'''

        if attribute.endswith('_date'):
            return DatetimeUtil.fromRfc3339(value)
        elif attribute == 'maja_mode':
            return MajaMode[value]

        # Default: call parent class
        return super().from_database_value(attribute, value)


    def get_last_inserted_job(self):
        '''
        Return the last job that has been inserted in the database, based on
        the highest L1C DIAS publication date.
        '''

        attribute_name = 'l1c_dias_publication_date'
        return super().select(attribute_name).max(attribute_name)


    def get_input_product_dias_publication_date(self):
        '''Return the L1C DIAS publication date.'''

        return self.l1c_dias_publication_date


    def get_input_product_esa_creation_date(self):
        '''Return the L1C ESA creation date.'''

        return self.l1c_esa_creation_date


    def get_input_product_esa_publication_date(self):
        '''Return the L1C ESA publication date.'''

        return self.l1c_esa_publication_date


    def set_input_product_esa_publication_date(self, value):
        '''Set the L1C ESA creation date.'''

        self.l1c_esa_publication_date = value


    def get_input_product_id(self):
        '''Return the L1C ID.'''

        return self.l1c_id


    def set_input_product_reference_job(self, value: bool):
        '''
        Set the L1C reference job parameter value.

        :param value: boolean value to notify if the job is a reference for a given L1C.
        '''

        self.l1c_reference_job = value


    def set_product_publication_date(self, product_name: str = None, publication_json: dict = None):
        '''
        Set the publication date of a given type of product.

        :param product_name: name of the product to update the publication date.
            This parameter is ot mandatory, but can be used if several products
            are generated by the same job to distinguish which product info should
            be updated.
        '''

        if product_name.lower() == "fsc":
            self.fsc_json_publication_date = datetime.utcnow()
            if publication_json is not None:
                self.fsc_infos = publication_json
        elif product_name.lower() == "rlie":
            self.rlie_json_publication_date = datetime.utcnow()
            if publication_json is not None:
                self.rlie_infos = publication_json


    def generated_a_product(self):
        '''Return a boolean to notify if the job did generate a product or not.'''

        return self.fsc_infos or self.rlie_infos


    def job_pre_insertion_setup(self, reprocessed_job=False):
        '''
        Perform set up actions for a given job, before its insertion in database.

        :param reprocessed_job: boolean notifying if the job is an old job part
            of a reprocessing campaign.
        '''

        if reprocessed_job:
            # Not a near real-time context
            self.nrt = False

            # Not the reference for this L1C
            self.l1c_reference_job = False
        else:
            # Always a near real-time context
            self.nrt = True

            # Set L2A product status
            self.l2a_status = L2aStatus.pending


    def set_job_unique_name(self):
        '''Set job unique name from the tile ID and measurement date.'''

        if self.name is None:
            self.name = self.tile_id
            self.name += '-%s' %self.measurement_date.strftime('%Y-%m-%d')


    def configure_single_job(self, logger_func):
        '''
        Perform L1C specific configuration, required for job processing.
        Here we fetch the previous job in the time-serie, compute the  status that
        should be set for this job. we also compute the MAJA mode that should be
        used for the job and the path under which the L2A product should be stored.

        :param logger_func: Logger instance
        '''
        # Set up output message
        message = None
        
        # Set job unique name based on tile ID and measurement date
        self.set_job_unique_name()

        # Find the previous job the current one depends on, if it exists
        previous_temporal_job = FscRlieJobUtil.find_previous_temporal_job_no_l2a_restriction(
            self,
            FscRlieJob(),
            allow_codated_jobs=False,
            logger_func=logger_func
            )

        # Compute job status function of the job dependencies
        status_to_set = self.update_job_status_function_of_dependencies(
            previous_temporal_job,
            logger_func=logger_func
            )

        # Perform additional configuration steps, required for the job
        #  processing, and specific to input product.
        if status_to_set == JobStatus.ready:
            # Set the MAJA mode the job should run with, if dependency completed
            return_message = FscRlieJobUtil.set_maja_mode(self, FscRlieJob, logger_func)

            if return_message == "Waiting to perform backward initialization":
                status_to_set = JobStatus.configured

            else:
                self.l2a_path_out = FscRlieJobUtil.build_path_from_bucket_and_object(
                    FscRlieJob.__SIP_DATA_BUCKET,
                    f'{self.tile_id}/L2A/reference/{self.l1c_id}/{self.maja_mode.name}'
                )
        elif status_to_set == JobStatus.internal_error:
            l2a_last_status_id = previous_temporal_job.last_status_id
            message = f"Backward_L2A_error:\nRetrieved last status for previous \
                    temporal job with id: '{previous_temporal_job.id}' for job with \
                    id {self.id} is {JobStatus(l2a_last_status_id)}.\nThis error can be raised\
                    if the chronological order has not been respected while creating L2As."

        return self, status_to_set, message


    def update_job_status_function_of_dependencies(self, previous_temporal_job, logger_func):
        '''
        Update jobs status function of their dependencies :
           - if job is independent -> set status to ready (wait for execution)
           - if job is dependent on an other job which status is processed or higher,
               -> set status to ready (wait for execution)
           - if job is dependent on an other job which status is not at least processed,
               or failed -> set status to configured (wait for dependency completion)

        :param job: single job for which we want to compute the status to be set.
        :param previous_temporal_job: previous job in the current one's time serie.
        :param logger_func: logger.debug or logger.info or ...
        '''

        # Check if the dependent job has compreleted its processing yet
        if previous_temporal_job:

            # Previous temporal job status prior to 'processed',
            # dependency is not satisfied, so job status is set to 'configured'
            if (isinstance(previous_temporal_job.last_status_id, int)
               and (previous_temporal_job.last_status_id < JobStatus.processed.value
                    or (self.reprocessing_context == "backward"
                        and previous_temporal_job.last_status_id in [
                            JobStatus.internal_error.value, JobStatus.external_error.value]
                        )
                    )
               ):

                logger_func.debug(
                    f"Job '{self.id}' is waiting for job '\
                    {previous_temporal_job.id}' to be completed!"
                    )
                return JobStatus.configured

            # Raise an error if the previous temporal job status is not/not properly set
            elif not isinstance(previous_temporal_job.last_status_id, int):
                logger_func.error(
                    f"Couldn't retrieve last status for previous temporal job with id \
                    '{previous_temporal_job.id}' for job with id {self.id} or status invalid."
                    )
                return JobStatus.internal_error
            # Dependency is satisfied so we check dependency's output product status
            else:

                # If dependency's output product has been generated, set status to 'ready'
                if previous_temporal_job.l2a_status == L2aStatus.generated.name:
                    return JobStatus.ready

                return JobStatus.configured

        # No dependency so we set status to 'ready'
        return JobStatus.ready


    def get_products_publication_jsons(self, publication_json_template: dict):
        '''
        Fill the JSONs to be sent to notify product publication for each product
        generated by this FSC RLIE job.

        :param dict_notifying_publication: JSON template to be filled before sending.
        '''

        products_publication_jsons = []

        # Iterate over each type of generated product
        for product in self.OUTPUT_PRODUCTS_LIST:

            # Create a new instance of the JSON template for each iteration
            dict_notifying_publication = copy.deepcopy(publication_json_template)

            # Json info set by SI_software/worker
            json_set_by_worker = None
            # Date set by worker when sending each product in the bucket
            product_generation_date = None

            if product == "fsc":
                # If product info were set by the worker, load them
                if self.fsc_infos is not None and self.fsc_path is not None:
                    json_set_by_worker = json.loads(self.fsc_infos)

                    # Retrieve parameters values specific to product type
                    if self.fsc_completion_date is not None:
                        product_generation_date = self.fsc_completion_date.strftime(
                            '%Y-%m-%dT%H:%M:%S.%f')

                    (s3_bucket, object_path) = FscRlieJobUtil.split_bucket_and_object_from_path(
                        self.fsc_path, prefix=True)

                    product_identifier = self.fsc_path

                    thumbnail = os.path.join(
                        FscRlieJobUtil.get_quicklook_bucket_path(object_path),
                        "thumbnail.png"
                    )
                else:
                    continue

            elif product == "rlie":
                # If product info were set by the worker, load them
                if self.rlie_infos is not None and self.rlie_path is not None:
                    json_set_by_worker = json.loads(self.rlie_infos)

                    # Retrieve parameters values specific to product type
                    if self.rlie_completion_date is not None:
                        product_generation_date = self.rlie_completion_date.strftime(
                            '%Y-%m-%dT%H:%M:%S.%f')

                    (s3_bucket, object_path) = FscRlieJobUtil.split_bucket_and_object_from_path(
                        self.rlie_path, prefix=True)

                    product_identifier = self.rlie_path

                    thumbnail = os.path.join(
                        FscRlieJobUtil.get_quicklook_bucket_path(object_path),
                        "thumbnail.png"
                    )
                else:
                    continue

            # Product info were not set by worker -> raise a warning
            if json_set_by_worker is None:
                self.logger.warning(
                    f"Couldn't publish job with id '{self.id}' '{product}' "\
                    "product's JSON as no information were set by the worker!"
                    )
                continue
            
            # Continue and else seem redundant, yet it happens that the first is missed, in which case
            # else statement is our safety net.
            else:
                # Retrieve information set by worker/SI_software
                try:
                    # Retrieve geometry information
                    dict_notifying_publication[
                        "resto"][
                            "geometry"][
                                "wkt"] = json_set_by_worker[
                                            "resto"][
                                                "geometry"][
                                                    "wkt"]

                    # Retrieve resource size information, provided in Kb, convert it in Bytes
                    dict_notifying_publication[
                        "resto"][
                            "properties"][
                                "resourceSize"] = 1024 * int(json_set_by_worker[
                                            "resto"][
                                                "properties"][
                                                    "resourceSize"])

                    # Retrieve resolution information
                    dict_notifying_publication[
                        "resto"][
                            "properties"][
                                "resolution"] = json_set_by_worker[
                                            "resto"][
                                                "properties"][
                                                    "resolution"]

                    # Retrieve cloud cover information
                    dict_notifying_publication[
                        "resto"][
                            "properties"][
                                "cloudCover"] = json_set_by_worker[
                                            "resto"][
                                                "properties"][
                                                    "cloudCover"]

                    # Retrieve "sensing time" set by SI software
                    datetime_pattern = '%Y-%m-%dT%H:%M:%S.%fZ'
                    if not json_set_by_worker["resto"]["properties"]["startDate"].endswith('Z'):
                        datetime_pattern = '%Y-%m-%dT%H:%M:%S.%f'
                    self.l1c_sensing_time = datetime.strptime(
                        json_set_by_worker[
                            "resto"][
                                "properties"][
                                    "startDate"],
                        datetime_pattern
                    )

                except KeyError:
                    self.logger.error(
                        f"Couldn't publish job with id '{self.id}' '{product}' "\
                        "product's JSON as information set by worker are not "\
                        "relevant!"
                        )
                
                # If json_set_by_worker is None, we will reach this exception
                # Redundant with if/else statement, yet we have had issues there
                # and it seems good to have multiple safety nets.
                except TypeError:
                    continue

                # Fill json to be sent
                try:
                    # Add product identifier information
                    dict_notifying_publication[
                        "resto"][
                            "properties"][
                                "productIdentifier"] = product_identifier

                    # Add product title information
                    dict_notifying_publication[
                        "resto"][
                            "properties"][
                                "title"] = os.path.basename(os.path.normpath(
                                    product_identifier))

                    # Add product start date information
                    dict_notifying_publication[
                        "resto"][
                            "properties"][
                                "startDate"] = self.l1c_sensing_time.strftime(
                                    '%Y-%m-%dT%H:%M:%S.%f')

                    # Add product completion date information
                    dict_notifying_publication[
                        "resto"][
                            "properties"][
                                "completionDate"] = product_generation_date

                    # Add product type information
                    dict_notifying_publication[
                        "resto"][
                            "properties"][
                                "productType"] = product.upper()

                    # Add system's version information
                    dict_notifying_publication[
                        "resto"][
                            "properties"][
                                "processingBaseline"] = re.search(
                                    r'V\d([\d]+)', product_identifier).group(0)

                    # Add product bucket information
                    dict_notifying_publication[
                        "resto"][
                            "properties"][
                                "s3_bucket"] = s3_bucket

                    # Add product thumbnail (quicklook) information
                    dict_notifying_publication[
                        "resto"][
                            "properties"][
                                "thumbnail"] = thumbnail

                    # Add mission information
                    dict_notifying_publication[
                        "resto"][
                            "properties"][
                                "mission"] = FscRlieJob.INPUT_PRODUCT_TYPE.upper()

                except TypeError:
                    # If json_set_by_worker is None, we will reach this exception
                    # Redundant with if/else statement, yet we have had issues there
                    # and it seems good to have multiple safety nets.
                    continue

                # Last resort to keep the job_publication afloat
                except Exception as error:
                    self.logger.error(
                        f"Couldn't publish job with id '{self.id}' '{product}' \
                        product's JSON as the following error occured during \
                        its setting:\n{error}"
                        )

                products_publication_jsons.append(dict_notifying_publication)

        return products_publication_jsons


    ################################################################
    # Start of static specifics methods

    @staticmethod
    def get_jobs_to_create(internal_database_parallel_request: int, logger):
        '''
        Request the DIAS for new L1C products and determine the list of jobs
        which should be created from it. We filter and order this list.

        :param internal_database_parallel_request: number of requests which can
            be performed in parallel to the database.
        :param logger: Logger instance.
        '''

        # Compute the time range between which we want to request the DIAS API
        (
            date_min,
            date_max,
            last_inserted_job
        ) = FscRlieJob.compute_dias_request_time_range(FscRlieJob, logger)

        if date_min:
            logger.info(
                f'Request new input products from the DIAS catalogue between dates \
                {DatetimeUtil.toRfc3339(date_min)} and \
                {DatetimeUtil.toRfc3339(date_max)}'
                )
        else:
            logger.info(
                f'Request new input products from the DIAS catalogue between dates \
                None and \
                {DatetimeUtil.toRfc3339(date_max)}'
                )
            

        # Request new input products in the DIAS catalogue
        s2_product_list = CreodiasUtil().request(
            logger,
            FscRlieJob.INPUT_PRODUCT_TYPE,
            FscRlieJob.__GEOMETRY,
            date_min,
            date_max,
            # Pass the max number of pages to request only if no min date is defined.
            max_requested_pages=FscRlieJob.__MAX_REQUESTED_PAGES
            if date_min is None else None)

        # Create an FscRlieJob for each Sentinel2 L1C product list
        jobs = []
        for s2_product in s2_product_list:
            # TODO add a constructor base on sentinel2_product objects
            jobs.append(FscRlieJob(
                    tile_id=s2_product.tile_id,
                    l1c_id=s2_product.product_id,
                    l1c_path=s2_product.product_path,
                    l1c_cloud_cover=s2_product.cloud_cover,
                    l1c_snow_cover=s2_product.snow_cover,
                    measurement_date=s2_product.measurement_date,
                    l1c_esa_creation_date=s2_product.esa_creation_date,
                    # There is also the 'update' param that we don't use.
                    l1c_dias_publication_date=s2_product.dias_publication_date))

        # Print statistics (for debugging)
        # FscRlieJobUtil.statistics(jobs, FscRlieJob, logger)

        # Keep only the jobs focusing on the appropriate tiles
        jobs = [j for j in jobs if j.tile_id in FscRlieJob.__TILE_IDS]

        # Keep only the jobs that do not already exist in the database
        # or that are re-published within 24h after their measurement
        jobs = FscRlieJobUtil.get_unique_jobs(jobs, last_inserted_job, FscRlieJob,
            internal_database_parallel_request, logger)

        # Filter reprocessed jobs which have been re-published recently,
        # but with a measurement date older than the operational system
        # processing start date (01/05/2020).
        s2_processing_start_date = SystemPrameters().get(logger).s2_processing_start_date
        s2_processing_start_date = datetime.strptime(s2_processing_start_date, "%Y-%m-%dT%H:%M:%S")

        jobs, reprocessed_jobs = FscRlieJobUtil.filter_reprocessed_input_products(
            jobs, s2_processing_start_date)

        # TODO batch insert jobs (all in once) in the database.
        # Meanwhile, the jobs are inserted one by one.
        # We sort them by the publication date in the DIAS, i.e. we insert the oldest first.
        # In case of error when inserting a job in the database, the next job insertions are
        # cancelled and we jump to the next call.
        # The max date between the inserted jobs will be used as a min date for the next call.
        # If the jobs are not inserted in chronological order, in case of error, jobs will
        # be missed in the next call.
        jobs.sort(key=lambda job: job.get_input_product_dias_publication_date())

        return jobs, reprocessed_jobs


    @staticmethod
    def get_existing_input_products(jobs, logger_func):
        '''
        return the list of jobs which already exist in the database among the ones provided.

        :param jobs: list of jobs to test if they exist in the database.
        :param logger_func: logger.debug or logger.info or ...
        '''

        existing_jobs = []

        for job in jobs:
            existing_jobs.extend(
                StoredProcedure.fsc_rlie_jobs_with_tile_date(
                    job.tile_id,
                    job.measurement_date.strftime('%Y-%m-%dT%H:%M:%S'),
                    FscRlieJob(),
                    logger_func=logger_func,
                    set_timeout=False
                )
            )
        return existing_jobs


    @staticmethod
    def get_jobs_without_esa_publication_date(logger_func):
        '''
        Retrieve all the FSC RLIE jobs inserted in the database which have
        no esa publication date set.

        :param logger_func: logger.debug or logger.info or ...
        '''

        return FscRlieJob().attribute_is(
            'l1c_esa_publication_date', 'null').get(logger_func)


    @staticmethod
    def configure_batch_jobs(jobs, logger_func):
        '''
        Perform L1C specific configuration, required for job processing.
        Here we set the ESA publication date, compute the job priority, and fix
        the ESA publication date for already processed jobs which would not have
        this information set.

        :param jobs: list of FSC RLIE jobs to be configured.
        :param logger_func: Logger instance
        '''

        # Update the jobs with information found in the ESA hub.
        jobs = FscRlieJobUtil.update_jobs_with_esa_info(jobs, logger_func)

        # Update the job priority levels
        jobs = FscRlieJobUtil.update_priority(jobs)

        # Fix ESA publication date for already configured jobs which are
        # lacking this parameter.
        FscRlieJob.fix_esa_publication_date(logger_func)

        # Order jobs function of their DIAS publication date
        jobs.sort(key=lambda job: job.get_input_product_dias_publication_date())

        return jobs


    @staticmethod
    def fix_esa_publication_date(logger):
        '''
        Set ESA publication date for existing FSC/RLIE jobs, which have
        already been configured but are lacking this parameter.

        :param job_type: specific type of job on which we want to ensure that the esa
            publication date are set for each job of this type present in the database.
        '''

        # Find all jobs which have no ESA publication date set
        jobs = FscRlieJob.get_jobs_without_esa_publication_date(logger_func=logger.debug)

        # Keep only jobs that already have been processed
        jobs = [
            job
            for job in jobs
            if job.last_status_id is not None
            and job.last_status_id >= JobStatus.done.value
        ]

        # Exit if no jobs
        if not jobs:
            logger.info('No configured jobs require an ESA publication date update')
            return

        logger.info(f'Update the ESA publication date of {len(jobs)} configured \
            {FscRlieJob.JOB_NAME} jobs')

        # Update the jobs with information found in the ESA hub, but do not change the job status.
        jobs = FscRlieJobUtil.update_jobs_with_esa_info(jobs, logger, status_update=False)

        for job in jobs:
            job.patch(patch_foreign=True, logger_func=logger.debug)