from enum import unique
import os
import re
import json
import copy
from functools import partial
from datetime import datetime, timedelta, date
from time import sleep

from .job_template import JobTemplate
from .system_parameters import SystemPrameters
from .job_status import JobStatus
from .worker_flavors import WorkerFlavors
from ....util.datetime_util import DatetimeUtil
from ....util.eea39_util import Eea39Util
from ....util.gfsc_job_util import GfscJobUtil
from ....util.hrsi_util import HrsiUtil
from ....util.sys_util import SysUtil
from ....util.exceptions import CsiInternalError, CsiExternalError
from ...rest.stored_procedure import StoredProcedure


class GfscJob(JobTemplate):
    '''
    Description of a job for the generation of one Daily Cumulative Gap-Filled Fractional Snow Cover (GFSC)
    from one or multiple FSC, WDS, SWS or GFSC product(s).

    :param name: Job name
    :param nrt: (boolean) Near Real-Time context ? If false: archive reprocessing.
    :param triggering_product_id: ID of the input product that triggered this job
    :param triggering_product_publication_date: (datetime) Publication date of the input product that triggered this job
    :param fsc_id_list: List of FSC product IDs
    :param fsc_publication_date_list: List of FSC product publication dates
    :param fsc_measurement_date_list: List of FSC product sensing dates
    :param wds_id_list: List of WDS product IDs
    :param wds_publication_date_list: List of WDS product publication dates
    :param wds_measurement_date_list: List of WDS product sensing dates
    :param sws_id_list: List of SWS product IDs
    :param sws_publication_date_list: List of SWS product publication dates
    :param sws_measurement_date_list: List of SWS product sensing dates
    :param gfsc_id_list: List of GFSC product IDs
    :param obsolete_product_id_list: List of input products which are not needed to be downloaded/processed
    :param gfsc_id: (string) GFSC Product ID
    :param gfsc_infos: (json) information relative to GFSC product.
    :param gfsc_path: (string) GFSC product path in the DIAS
    :param sensing_start_date: (datetime) Datatake sensing start time (aggregation)
    :param sensing_end_date: (datetime) Datatake sensing end time (aggregation)
    :param completion_date: (datetime) GFSC publication date in the bucket.
    :param product_date: (datetime) GFSC Product date
    :param curation_timestamp: (datetime) Time of the curation of input products (seconds from Unix Time)
    :param aggregation_timespan: (int) Number of days backwards to aggregate the products from the product day
    :param gfsc_json_publication_date: (datetime) GFSC JSON publication date on RabbitMQ endpoint.
    :param overriding_job_id: (string) ID of the job that has overridden this one by finding it obsolete
    :param reprocessing_context: (string) Notify the reprocessing context for a non-NRT job
    '''

    # Database table name
    __TABLE_NAME = "gfsc_jobs"

    # Class job name
    JOB_NAME = "GFSC"

    # Input product type
    INPUT_PRODUCTS_TYPE = ['fsc','wds','sws','gfsc']

    # Name of the Nomad job processing the present job type
    NOMAD_JOB_NAME = "gfsc-processing"

    # Worker flavor required for the present job type processing
    WORKER_FLAVOR_NAME = WorkerFlavors.small.value

    # Tile used for the prototype.
    # __TILE_RESTRICTION = ['30TYN', '32TLR', '32TMS', '33WWR', '33WXS', '34WBD', 
    #                       '32TLS', '32TLT', '38SLJ', '38SMJ', '38TMK']
    __TILE_RESTRICTION = None

    # List of all the EEA39 Sentinel-2 tile IDs or filtered accordint __TILE_RESTRICTION.
    __TILE_IDS = Eea39Util.get_tiles(tile_restriction=__TILE_RESTRICTION)

    # EEA39 Area Of Interest (AOI) to request, in WGS84 projection.
    __GEOMETRY = Eea39Util.get_geometry(tile_restriction=__TILE_RESTRICTION)

    # Max number of pages to request (if None: infinite).
    __MAX_REQUESTED_PAGES = None# if __TILE_RESTRICTION is None else 20

    # Keep track of the last GFSC job inserted in the database.
    LAST_INSERTED_JOB = None

    # Keep track of the day to create daily jobs without input products
    __DAILY_JOB_DATE = None

    # Time after the input product acquisition within which we replace any job
    # already generated from this input product.
    # DUPLICATE_INPUT_PRODUCT_VALID_TIME = timedelta(days=1)

    # Operational system processing start date (01/05/2020)
    __PROCESSING_START_DATE = datetime(2020, 5, 1)

    # INPUT_PRODUCT_TYPE = 'GFSC'

    # Name of the stored procedure used to retrieve FscRlieJobs with a given status
    GET_JOBS_WITH_STATUS_PROCEDURE_NAME = "gfsc_jobs_with_last_status"

    def __init__(self, **kwds):
        self.nrt = None
        self.triggering_product_id = None
        self.triggering_product_publication_date = None
        self.fsc_id_list = None
        self.fsc_publication_date_list = None
        self.fsc_measurement_date_list = None
        self.wds_id_list = None
        self.wds_publication_date_list = None
        self.wds_measurement_date_list = None
        self.sws_id_list = None
        self.sws_publication_date_list = None
        self.sws_measurement_date_list = None
        self.gfsc_id_list = None
        self.obsolete_product_id_list = None
        self.gfsc_infos = None
        self.gfsc_id = None
        self.gfsc_path = None
        self.sensing_start_date = None
        self.sensing_end_date = None
        self.completion_date = None
        self.product_date = None
        self.curation_timestamp = None
        self.aggregation_timespan = None
        self.gfsc_json_publication_date = None
        self.reprocessing_context = None
        self.overriding_job_id = None

        # Call the parent constructor AFTER all the attributes are initialized with None
        super().__init__(GfscJob.__TABLE_NAME)

        # Attribute values given by the caller
        for key, value in kwds.items():
            setattr(self, key, value)


    ################################################################
    # Start of specific methods

    def from_database_value(self, attribute, value):
        '''Parse a string value as it was inserted in the database.'''

        if attribute.endswith('_date') or attribute.endswith('_timestamp'):
            return DatetimeUtil.fromRfc3339(value)
        elif attribute.endswith('id_list'):
            if value is None or value == '':
                return []
            else:
                return value.split(';')

        # Default: call parent class
        return super().from_database_value(attribute, value)

    def to_database_value(self, attribute, value):
        '''Return a value as it must be inserted in the database.'''

        # Convert datetimes to string
        if isinstance(value, datetime):
            return DatetimeUtil.toRfc3339(value)

        # Convert list to string
        elif isinstance(value, list):
            if value is None or value == []:
                return None
            else:
                return ';'.join(value)

        # Default: call parent class
        return super().to_database_value(attribute, value)


    def set_product_id(self):
        '''Set initial ID of the output GFSC product. Missions and 
        processingBaseline is set later by the docker image'''
        if self.gfsc_id is None:
            self.gfsc_id = '_'.join([
                'GFSC',
                self.product_date.strftime('%Y%m%d') + '-' + str(self.aggregation_timespan).zfill(3),
                'missions',
                self.tile_id,
                'processingBaseline',
                str(int(self.curation_timestamp.timestamp()))
                ])


    def set_job_unique_name(self):
        '''Set job unique name from the tile ID, product date, curation timestamp and triggering product id.'''

        if self.name is None:
            self.name = self.tile_id
            self.name += '-%s' %self.product_date.strftime('%Y-%m-%d')
            self.name += '-%s' %self.curation_timestamp.strftime('%Y%m%dT%H%M%S')
            self.name += '-%s' %self.triggering_product_id

    def get_input_product_dias_publication_date(self):
        '''Return the triggering input product publication date.'''

        return DatetimeUtil.fromRfc3339(DatetimeUtil.toRfc3339(self.triggering_product_publication_date))

    def get_product_date(self):
        return self.product_date


    def get_last_inserted_job(self):
        '''
        Return the last job that has been inserted in the database, based on
        the highest triggering product DIAS publication date.
        '''

        attribute_name = 'triggering_product_publication_date'
        return super().select(attribute_name).max(attribute_name)


    def set_product_publication_date(self, product_name: str, publication_json: dict = None):
        '''
        Set the publication date of a given type of product.

        :param product_name: name of the product to update the publication date.
        '''

        self.gfsc_json_publication_date = datetime.utcnow()
        if publication_json is not None:
            self.gfsc_infos = publication_json


    def generated_a_product(self):
        '''Return a boolean to notify if the job did generate a product or not.'''
        return False if self.gfsc_infos == '' else True


    def job_pre_insertion_setup(self, reprocessed_job=False):
        '''
        Perform set up actions for a given job, before its insertion in database.

        :param reprocessed_job: boolean notifying if the job is an old job part
            of a reprocessing campaign.
        '''

        if reprocessed_job:
            # Not a near real-time context
            self.nrt = False
        else:
            # Always a near real-time context
            self.nrt = True

    def configure_single_job(self, logger_func):
        '''
        Perform specific configuration, required for job processing.
        Here we fetch the previous job in the time-serie, compute the  status that
        should be set for this job.

        :param logger_func: Logger instance
        '''
        # Set up output message
        message = None
        
        # Set job unique name from the tile ID, product date, curation timestamp and triggering_product_id
        self.set_job_unique_name()

        # Provisional/Nominal input product handling
        if GfscJobUtil.get_input_product_mode(self.triggering_product_id) == '1' and GfscJobUtil.get_input_product_mode_toggled_id(self.triggering_product_id) in self.fsc_id_list + self.wds_id_list + self.sws_id_list:
            message = 'Job triggered by nomimal products whose provisional versions used before in NRT. Cancelling job.'
            logger_func.info(message)
            return self, JobStatus.cancelled, message

        # Cancel obsolete and duplicate (same curation date, different trigger) jobs
        same_tile_date_jobs = StoredProcedure.get_gfsc_jobs_with_status_product_date_tile(
                [JobStatus.initialized,JobStatus.configured,JobStatus.ready],
                self.product_date.strftime('%Y-%m-%dT%H:%M:%S'),
                self.tile_id,
                GfscJob(),
                logger_func.info)

        for job in same_tile_date_jobs:
            if job.curation_timestamp >= self.curation_timestamp and job.id != self.id:
                message = 'A newer or same curation time job exists for the same tile and product date. Cancelling job.'
                logger_func.info(message)
                self.overriding_job_id = job.id
                return self, JobStatus.cancelled, message

        # Check if input product is not empty and it is not a GFSC triggered job
        if len(self.fsc_id_list + self.wds_id_list + self.sws_id_list) == 0 and GfscJobUtil.get_input_product_type(self.triggering_product_id) != 'gfsc':
            logger_func.info('The job has been triggerred but no input product has returned from the query during job creation. Attempting to fetch input products again.')
            date_max = self.curation_timestamp if self.product_date == datetime(self.curation_timestamp.year,self.curation_timestamp.month,self.curation_timestamp.day) else self.product_date + timedelta(days=1) - timedelta(microseconds=1)
            date_min = date_max.replace(hour=0,minute=0,second=0,microsecond=0)-timedelta(days=self.aggregation_timespan-1)
            logger_func.info(
                'Request input products from the DIAS catalogue between measurement dates %s and %s for tile ID %s' %
                ((DatetimeUtil.toRfc3339(date_min) if date_min else 'None'),
                DatetimeUtil.toRfc3339(date_max),
                self.tile_id))
            date_min = DatetimeUtil.fromRfc3339(DatetimeUtil.toRfc3339(date_min))
            date_max = DatetimeUtil.fromRfc3339(DatetimeUtil.toRfc3339(date_max))
            fsc_product_list = HrsiUtil().request(
                    logger_func,
                    "fsc",
                    self.__GEOMETRY,
                    None,
                    None,
                    other_params={
                        'productIdentifier':'%'+self.tile_id+'%', 
                        'startDate':date_min.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'completionDate':date_max.strftime('%Y-%m-%dT%H:%M:%SZ')},
                    # Pass the max number of pages to request only if no min date is defined.
                    max_requested_pages=None)
            wds_product_list = HrsiUtil().request(
                    logger_func,
                    "wds",
                    self.__GEOMETRY,
                    None,
                    None,
                    other_params={
                        'productIdentifier':'%'+self.tile_id+'%', 
                        'startDate':date_min.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'completionDate':date_max.strftime('%Y-%m-%dT%H:%M:%SZ')},
                    # Pass the max number of pages to request only if no min date is defined.
                    max_requested_pages=None)
            sws_product_list = HrsiUtil().request(
                    logger_func,
                    "sws",
                    self.__GEOMETRY,
                    None,
                    None,
                    other_params={
                        'productIdentifier':'%'+self.tile_id+'%', 
                        'startDate':date_min.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'completionDate':date_max.strftime('%Y-%m-%dT%H:%M:%SZ')},
                    # Pass the max number of pages to request only if no min date is defined.
                    max_requested_pages=None)
            logger_func.info(
                'Filtering input products from the DIAS catalogue between measurement dates %s and %s' %
                ((DatetimeUtil.toRfc3339(date_min) if date_min else 'None'),
                DatetimeUtil.toRfc3339(date_max)))
            fsc_product_list = GfscJob.filter_product_list(fsc_product_list,measurement_date_min=date_min,measurement_date_max=date_max)
            wds_product_list = GfscJob.filter_product_list(wds_product_list,measurement_date_min=date_min,measurement_date_max=date_max)
            sws_product_list = GfscJob.filter_product_list(sws_product_list,measurement_date_min=date_min,measurement_date_max=date_max)
            self.fsc_id_list=[product.product_id for product in fsc_product_list]
            self.fsc_publication_date_list=[DatetimeUtil.toRfc3339(product.publication_date) for product in fsc_product_list]
            self.fsc_measurement_date_list=[DatetimeUtil.toRfc3339(product.measurement_date) for product in fsc_product_list]
            self.wds_id_list=[product.product_id for product in wds_product_list]
            self.wds_publication_date_list=[DatetimeUtil.toRfc3339(product.publication_date) for product in wds_product_list]
            self.wds_measurement_date_list=[DatetimeUtil.toRfc3339(product.measurement_date) for product in wds_product_list]
            self.sws_id_list=[product.product_id for product in sws_product_list]
            self.sws_publication_date_list=[DatetimeUtil.toRfc3339(product.publication_date) for product in sws_product_list]
            self.sws_measurement_date_list=[DatetimeUtil.toRfc3339(product.measurement_date) for product in sws_product_list]

            if len(self.fsc_id_list + self.wds_id_list + self.sws_id_list) == 0:
                logger_func.info('Input products still cannot be found. Leaving the job as configured.')
                return self, JobStatus(self.last_status_id), message

        # If there is WDS/SWS arrived for the same day or 3h passed
        # mark as ready
        # If there is new WDS/SWS, this job is already overridden
        if GfscJobUtil.get_input_product_type(self.triggering_product_id) == 'fsc':
            for product_id in self.wds_id_list+self.sws_id_list:
                if GfscJobUtil.get_input_product_date(product_id) == GfscJobUtil.get_input_product_date(self.triggering_product_id):
                    status_to_set = JobStatus.ready
                    break
            if self.triggering_product_publication_date + timedelta(hours=3) <= datetime.utcnow():
                status_to_set = JobStatus.ready
            else:
                status_to_set = JobStatus(self.last_status_id)
        else:
            status_to_set = JobStatus.ready

        if status_to_set == JobStatus.ready:
            if self.gfsc_id_list is None:
                # Add GFSC products as input if job is ready
                logger_func.info('Requesting input GFSC products to add to the job.')
                # startDate of GFSC is the sensing time of earliest input product
                # Request 2 x aggregation timespan to cover all time possible
                date_min = self.product_date - timedelta(days=2*self.aggregation_timespan)
                date_max = self.curation_timestamp if self.product_date == datetime(self.curation_timestamp.year,self.curation_timestamp.month,self.curation_timestamp.day) else self.product_date.replace(hour=23,minute=59,second=59)
                gfsc_id_list = HrsiUtil().request(
                        logger_func,
                        "gfsc",
                        self.__GEOMETRY,
                        None,
                        None,
                        other_params={'productIdentifier':'%'+self.tile_id+'%','startDate':date_min.strftime('%Y-%m-%dT%H:%M:%SZ'),'completionDate':date_max.strftime('%Y-%m-%dT%H:%M:%SZ')},
                        # Pass the max number of pages to request only if no min date is defined.
                        max_requested_pages=None
                        if self.product_date - timedelta(days=self.aggregation_timespan-1) is None else None)
                # Get only latest
                # If this changes in the future, RunGfscWorker.find_obsolete_input_products should be updated
                self.gfsc_id_list = sorted(gfsc_id_list, key=lambda x: int(x.product_id.split('_')[-1]), reverse=True)[0]

            # # Initialize product ID. This will be updated later in post_processing.
            self.set_product_id()

        return self, status_to_set, message

    def get_fsc_product_publication_date(self,fsc_product_id,logger_func):
        '''
        Get publication date of a FSC product from REST API

        :param fsc_product_id: ID of the FSC product
        :param logger_func: logger.debug or logger.info or ...
        '''
        fsc_product_list = HrsiUtil().request(
                    logger_func,
                    "fsc",
                    GfscJob.__GEOMETRY,
                    None,
                    None,
                    other_params={'productIdentifier':fsc_product_id},
                    # Pass the max number of pages to request only if no min date is defined.
                    max_requested_pages=None)
        return fsc_product_list[0].publication_date

    def get_products_publication_jsons(self, publication_json_template: dict):
        '''
        Fill the json to be sent to notify product publication.

        :param publication_json_template: JSON template to be filled before sending.
        '''

        # Create a new instance of the JSON template for each iteration
        dict_notifying_publication = copy.deepcopy(publication_json_template)

        # Json info set by SI_software/worker
        json_set_by_worker = None
        product = 'gfsc'

        # If product info were set by the worker, load them
        if self.gfsc_infos is not None and self.gfsc_path is not None:
            json_set_by_worker = json.loads(self.gfsc_infos)

        # Product info were not set by worker -> raise a warning
        if json_set_by_worker is None:
            self.logger.warning("Couldn't publish job with id '%s' '%s' "\
                "product's JSON as no information were set by the worker!"\
                %(self.id, product))
            return []

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
                        "resourceSize"] = int(json_set_by_worker[
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

        except KeyError:
            self.logger.error("Couldn't publish job with id '%s' '%s' "\
                "product's JSON as information set by worker are not "\
                "relevant!" %(self.id, product))
            return []

        # Fill json to be sent
        try:
            # Add the current product collection name
            dict_notifying_publication[
                "collection_name"] = "HR-S&I"

            # Add type
            dict_notifying_publication["resto"]["type"] = "Feature"

            # Add product identifier information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "productIdentifier"] = self.gfsc_path

            # Add product title information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "title"] = self.gfsc_id

            # Add product organistation name information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "organisationName"] = "EEA"

            # Add product start date information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "startDate"] = self.sensing_start_date.strftime(
                            '%Y-%m-%dT%H:%M:%S.%f')

            # Add product completion date information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "completionDate"] = self.completion_date.strftime('%Y-%m-%dT%H:%M:%S.%f')

            # Add product type information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "productType"] = product.upper()

            # Add system's version information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "processingBaseline"] = self.gfsc_id.split('_')[4]

            # Add product host base information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "host_base"] = "s3.waw2-1.cloudferro.com"

            # Add product bucket information
            (s3_bucket, object_path) = GfscJobUtil.split_bucket_and_object_from_path(
                self.gfsc_path, prefix=True)
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "s3_bucket"] = s3_bucket

            # Add product thumbnail (quicklook) information
            thumbnail = os.path.join(
                GfscJobUtil.get_quicklook_bucket_path(object_path),
                "thumbnail.png"
            )
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "thumbnail"] = thumbnail

            # Add mission information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "mission"] = self.gfsc_id.split('_')[2]

        except Exception as e:
            self.logger.error("Couldn't publish job with id '%s' '%s' "\
                "product's JSON as the following error occured during "\
                "its setting : \n%s" %(self.id, product, e))
            return []

        return [dict_notifying_publication]


    ################################################################
    # Start of static specifics methods

    @staticmethod
    def get_input_product_search_default_duration_in_days(logger=None):
        '''
        return the FSC, WDS, SWS, GFSC search default aggregation duration (in days) stored in the system parameters.
        :param logger: logger.debug or logger.info or ...
        '''
        # TODO [Minor] uniformized with method in job_template.py
        return SystemPrameters().get(logger).gapfilling_search_default_duration_in_days

    @staticmethod
    def compute_dias_request_time_range(job_type_class, logger):
        '''
        Compute the time range on which should be performed the request to the
        DIAS to look for new input products based on the last job inserted in
        the database. If no job were inserted yet we perform a search for the
        current day.


        :param job_type_class: specific job type class.
        :param logger: Logger instance.
        '''

        # Max date = now
        date_max = datetime.utcnow().replace(microsecond=0)

        # Min date = max publication date in the DIAS between all the
        # products that already exist in the database.
        # Only request database if we don't have information on last inserted job
        if job_type_class.LAST_INSERTED_JOB:
            job_dates = [job_type_class.LAST_INSERTED_JOB]
            last_inserted_job = None
        else:
            job_dates = job_type_class().get_last_inserted_job().get(logger_func=logger.debug)
            last_inserted_job = None

        if isinstance(job_dates, list) and len(job_dates) > 0:
            last_inserted_job = job_dates[0]
            date_min = last_inserted_job.get_input_product_dias_publication_date()
            
            # Remove 1 second in the case of multiple products were published
            # at the exact same moment of the last request and if the min date
            # is exclusive.
            date_min = date_min - timedelta(seconds=1)

        else:
            # This is a value to use for search of input
            # products at the starting of the system, i.e. when no product
            # has ever been processed by our processing and the database is empty.
            # There is no argument to add here, so daily search in enough.
            # TODO [Minor] : Rmove code added for Part2 operational phase start
            date_min = datetime(date_max.year,date_max.month,date_max.day) - timedelta(days=1)
            # date_min = datetime(date_max.year,date_max.month,date_max.day) - timedelta(days=44)
            # date_min = datetime(date_max.year,date_max.month,date_max.day)

        return date_min, date_max, last_inserted_job

    @staticmethod
    def filter_product_list(product_list,tile_id=None,publication_date_min=None,publication_date_max=None,measurement_date_min=None,measurement_date_max=None):
        '''
        Filter the list of the HrsiProducts returning from the large REST API query
        according to tile_id, publication (for new input products) and 
        measurement date (for input products to process)

        :param product_list: ([HrsiProduct]) List of products
        :param tile_id: Tile ID
        :param publication_date_min: Earliest publication date
        :param publication_date_max: Latest publication date
        :param measurement_date_min: Earliest measurement date
        :param measurement_date_max: Latest measurement date
        '''
        if tile_id is not None:
            product_list = [product for product in product_list if product.tile_id == tile_id]
        if publication_date_min is not None:
            product_list = [product for product in product_list if product.publication_date > publication_date_min]
        if publication_date_max is not None:
            product_list = [product for product in product_list if product.publication_date < publication_date_max]
        if measurement_date_min is not None:
            product_list = [product for product in product_list if product.measurement_date >= measurement_date_min]
        if measurement_date_max is not None:
            product_list = [product for product in product_list if product.measurement_date <= measurement_date_max]
        return product_list

    @staticmethod
    def get_jobs_to_create(internal_database_parallel_request: int, logger):
        '''
        Request the DIAS for new FSC, WDS and SWS products and determine the list of jobs
        which should be created from it.

        :param internal_database_parallel_request: number of requests which can
            be performed in parallel to the database.
        :param logger: Logger instance.
        '''

        # TODO To be updated into a long-term solution,
        # Set a constant startDate limit to prevent requesting reprocessing products
        nrt_start_date_limit = DatetimeUtil.fromRfc3339(DatetimeUtil.toRfc3339(datetime(2022, 1, 1)))

        aggregation_timespan = GfscJob.get_input_product_search_default_duration_in_days(logger.debug)
        jobs = []

        # Create daily jobs if no input product for yesterday
        curation_timestamp = datetime.utcnow()
        daily_job_date = datetime(curation_timestamp.year,curation_timestamp.month,curation_timestamp.day)
        daily_job_date -= timedelta(days=1)
        # Do this only at night because this may take up to 1 hour and it is unnecessary if there was an interruption
        if curation_timestamp.hour in [22,23,0,1,2] and (GfscJob.__DAILY_JOB_DATE is None or daily_job_date > GfscJob.__DAILY_JOB_DATE):
            if GfscJob.__DAILY_JOB_DATE is not None:
                daily_job_date = GfscJob.__DAILY_JOB_DATE + timedelta(days=1)
            else:
                gfsc_daily_jobs_creation_start_date = SystemPrameters().get(logger.debug).gfsc_daily_jobs_creation_start_date
                if gfsc_daily_jobs_creation_start_date is not None and len(gfsc_daily_jobs_creation_start_date) > 0:
                    daily_job_date = DatetimeUtil.fromRfc3339(gfsc_daily_jobs_creation_start_date)

            logger.info('Creating daily jobs on %s for remaning tiles' % daily_job_date.strftime('%Y-%m-%d'))
            publication_date_min = daily_job_date
            publication_date_max = daily_job_date + timedelta(days=1,seconds=-1)
            logger.info(
                    'Request new input products from the DIAS catalogue between measurement dates %s and %s' %
                    ((DatetimeUtil.toRfc3339(publication_date_min) if publication_date_min else 'None'),
                    DatetimeUtil.toRfc3339(publication_date_max)))
            publication_date_min = DatetimeUtil.fromRfc3339(DatetimeUtil.toRfc3339(publication_date_min))
            publication_date_max = DatetimeUtil.fromRfc3339(DatetimeUtil.toRfc3339(publication_date_max))
            fsc_product_list = HrsiUtil().request(
                    logger,
                    "fsc",
                    GfscJob.__GEOMETRY,
                    None,
                    None,
                    other_params={'startDate':publication_date_min.strftime('%Y-%m-%dT%H:%M:%SZ'),'completionDate':publication_date_max.strftime('%Y-%m-%dT%H:%M:%SZ')})
            wds_product_list = HrsiUtil().request(
                    logger,
                    "wds",
                    GfscJob.__GEOMETRY,
                    None,
                    None,
                    other_params={'startDate':publication_date_min.strftime('%Y-%m-%dT%H:%M:%SZ'),'completionDate':publication_date_max.strftime('%Y-%m-%dT%H:%M:%SZ')})
            sws_product_list = HrsiUtil().request(
                    logger,
                    "sws",
                    GfscJob.__GEOMETRY,
                    None,
                    None,
                    other_params={'startDate':publication_date_min.strftime('%Y-%m-%dT%H:%M:%SZ'),'completionDate':publication_date_max.strftime('%Y-%m-%dT%H:%M:%SZ')})
            logger.info('%s input product for the day found.' % len(fsc_product_list+wds_product_list+sws_product_list))
            if len(fsc_product_list+wds_product_list+sws_product_list) == 0:
                logger.info('There has been probably an interruption in NRT system. Daily jobs will be not created at this time.')
            else:
                publication_date_min -= timedelta(days=aggregation_timespan*2)
                publication_date_min = DatetimeUtil.fromRfc3339(DatetimeUtil.toRfc3339(publication_date_min))
                logger.info(
                    'Request GFSC products from the DIAS catalogue between measurement dates %s and %s' %
                    ((DatetimeUtil.toRfc3339(publication_date_min) if publication_date_min else 'None'),
                    DatetimeUtil.toRfc3339(publication_date_max)))
                gfsc_product_list = HrsiUtil().request(
                        logger,
                        "gfsc",
                        GfscJob.__GEOMETRY,
                        None,
                        None,
                        other_params={'startDate':publication_date_min.strftime('%Y-%m-%dT%H:%M:%SZ'),'completionDate':publication_date_max.strftime('%Y-%m-%dT%H:%M:%SZ')})
                
                all_tile_id_list = Eea39Util.get_tiles(tile_restriction=None)
                all_tile_id_list = ['T'+tile for tile in all_tile_id_list]
                all_tile_id_list = set(all_tile_id_list)
                input_tile_id_list = [product.tile_id for product in fsc_product_list+wds_product_list+sws_product_list]
                input_tile_id_list = set(input_tile_id_list)
                logger.info('No input products found for %s of %s tiles.' % (len(all_tile_id_list-input_tile_id_list),len(all_tile_id_list)))

                logger.info('Keep only latest GFSC product for each no-product-tile.')
                gfsc_tile_id_list = [product.tile_id for product in gfsc_product_list]
                gfsc_tile_id_list = list(set(gfsc_tile_id_list)-input_tile_id_list)
                gfsc_product_list = [sorted(GfscJob.filter_product_list(gfsc_product_list,tile_id=tile_id), key=lambda x: int(x.product_id.split('_')[-1]), reverse=True)[0] for tile_id in gfsc_tile_id_list]
                logger.info('%s products found.' % len(gfsc_product_list))

                logger.info('Creating daily jobs for %s tiles.' % len(all_tile_id_list-input_tile_id_list))                  
                (triggering_product_publication_date, thrash, last_inserted_job) = GfscJob.compute_dias_request_time_range(GfscJob, logger)
                # This is used so that next call date_min is right
                if last_inserted_job is not None:
                    triggering_product_publication_date = last_inserted_job.triggering_product_publication_date
                for tile_id in list(set(gfsc_tile_id_list)-set(input_tile_id_list)):
                    product = GfscJob.filter_product_list(gfsc_product_list,tile_id=tile_id)
                    if product == []:
                        print('Cannot find input product for tile %s' % tile_id)
                        continue
                    product = product[0]
                    # Do not create the job if input GFSC is too old. This can happen for few day after restart if there is an interuption in NRT operations
                    if DatetimeUtil.fromRfc3339(DatetimeUtil.toRfc3339(daily_job_date)) >= DatetimeUtil.fromRfc3339(DatetimeUtil.toRfc3339(product.measurement_date)) + timedelta(days=aggregation_timespan):
                        logger.info('Input GFSC is too old (%s for day %s). Skipping job.' %(product.measurement_date,daily_job_date))
                        continue
                    # Do not create the daily job if a GFSC job already exists on the same 'product_date'
                    product_product_date = datetime(product.measurement_date.year,product.measurement_date.month,product.measurement_date.day)
                    if product_product_date == daily_job_date:
                        continue
                    jobs.append(GfscJob(
                        tile_id=tile_id,
                        product_date=daily_job_date,
                        curation_timestamp=curation_timestamp,
                        aggregation_timespan=aggregation_timespan,
                        triggering_product_id=product.product_id,
                        # This is used so that next call date_min is right
                        triggering_product_publication_date=triggering_product_publication_date,
                        fsc_id_list=[],
                        fsc_publication_date_list=[],
                        fsc_measurement_date_list=[],
                        wds_id_list=[],
                        wds_publication_date_list=[],
                        wds_measurement_date_list=[],
                        sws_id_list=[],
                        sws_publication_date_list=[],
                        sws_measurement_date_list=[],
                        gfsc_id_list=[product.product_id]
                    ))
                logger.info('%i jobs added to job list.',len(jobs))

                # This is moved here so that if something fails, it can be tried again (at night).
                GfscJob.__DAILY_JOB_DATE = daily_job_date

        # Compute the time range between which we want to request the DIAS API
        (
            publication_date_min,
            publication_date_max,
            last_inserted_job
        ) = GfscJob.compute_dias_request_time_range(GfscJob, logger)
        publication_date_max = publication_date_max
        curation_timestamp = publication_date_max
        # Wait 1 second to leave some time for products published at that moment
        sleep(1)

        logger.info(
            'Request new input products from the DIAS catalogue between publication dates %s and %s' %
            ((DatetimeUtil.toRfc3339(publication_date_min) if publication_date_min else 'None'),
            DatetimeUtil.toRfc3339(publication_date_max)))
        
        publication_date_min = DatetimeUtil.fromRfc3339(DatetimeUtil.toRfc3339(publication_date_min))
        publication_date_max = DatetimeUtil.fromRfc3339(DatetimeUtil.toRfc3339(publication_date_max))
        
        triggering_product_list = HrsiUtil().request(
                logger,
                "fsc",
                GfscJob.__GEOMETRY,
                publication_date_min,
                publication_date_max,
                # other_params={})
                # Take some margin regarding the last inserted job date in case some jobs 
                # processing/publication were delayed
                other_params={'startDate':f"{DatetimeUtil.toRfc3339(publication_date_min - timedelta(days=aggregation_timespan))}", 
                    'completionDate':f"{DatetimeUtil.toRfc3339(publication_date_max)}"})
        triggering_product_list += HrsiUtil().request(
                logger,
                "wds",
                GfscJob.__GEOMETRY,
                publication_date_min,
                publication_date_max,
                # other_params={})
                other_params={'startDate':f"{DatetimeUtil.toRfc3339(min(publication_date_min, nrt_start_date_limit))}", 
                    'completionDate':f"{DatetimeUtil.toRfc3339(publication_date_max)}"})
        
        triggering_product_list += HrsiUtil().request(
                logger,
                "sws",
                GfscJob.__GEOMETRY,
                publication_date_min,
                publication_date_max,
                # other_params={})
                other_params={'startDate':f"{DatetimeUtil.toRfc3339(min(publication_date_min, nrt_start_date_limit))}", 
                    'completionDate':f"{DatetimeUtil.toRfc3339(publication_date_max)}"})

        # Remove triggering product of last_inserted_job
        # That one is pretty much always included due to API timing
        triggering_product_id_list = [product.product_id for product in triggering_product_list]
        if last_inserted_job is not None and last_inserted_job.triggering_product_id in triggering_product_id_list:
            del triggering_product_list[triggering_product_id_list.index(last_inserted_job.triggering_product_id)]

        if triggering_product_list == []:
                return jobs,[]
        else:
            logger.info('%s new input products found.' % len(triggering_product_list))

        publication_date_min = sorted(triggering_product_list, key=lambda x: x.publication_date)[0].publication_date
        publication_date_min = publication_date_min.replace(hour=0,minute=0,second=0,microsecond=0)-timedelta(days=aggregation_timespan)
        logger.info(
                'Request new input products from the DIAS catalogue between publication dates %s and %s' %
                ((DatetimeUtil.toRfc3339(publication_date_min) if publication_date_min else 'None'),
                DatetimeUtil.toRfc3339(publication_date_max)))
        publication_date_min = DatetimeUtil.fromRfc3339(DatetimeUtil.toRfc3339(publication_date_min))
        publication_date_max = DatetimeUtil.fromRfc3339(DatetimeUtil.toRfc3339(publication_date_max))
        fsc_product_large_list = HrsiUtil().request(
                logger,
                "fsc",
                GfscJob.__GEOMETRY,
                publication_date_min,
                publication_date_max,
                # other_params={})
                # Take some margin regarding the last inserted job date in case some jobs 
                # processing/publication were delayed
                other_params={'startDate':f"{DatetimeUtil.toRfc3339(publication_date_min - timedelta(days=aggregation_timespan))}", 
                    'completionDate':f"{DatetimeUtil.toRfc3339(publication_date_max)}"})
        wds_product_large_list = HrsiUtil().request(
                logger,
                "wds",
                GfscJob.__GEOMETRY,
                publication_date_min,
                publication_date_max,
                # other_params={})
                other_params={'startDate':f"{DatetimeUtil.toRfc3339(min(publication_date_min, nrt_start_date_limit))}", 
                    'completionDate':f"{DatetimeUtil.toRfc3339(publication_date_max)}"})
        sws_product_large_list = HrsiUtil().request(
                logger,
                "sws",
                GfscJob.__GEOMETRY,
                publication_date_min,
                publication_date_max,
                # other_params={})
                other_params={'startDate':f"{DatetimeUtil.toRfc3339(min(publication_date_min, nrt_start_date_limit))}", 
                    'completionDate':f"{DatetimeUtil.toRfc3339(publication_date_max)}"})

        for product in triggering_product_list:
            tile_id = product.tile_id
            product_date = datetime(product.measurement_date.year,product.measurement_date.month,product.measurement_date.day)
            publication_date_max = curation_timestamp if product_date == datetime(curation_timestamp.year,curation_timestamp.month,curation_timestamp.day) else product_date + timedelta(days=1) - timedelta(microseconds=1)
            publication_date_min = publication_date_max.replace(hour=0,minute=0,second=0,microsecond=0)-timedelta(days=aggregation_timespan-1)
            logger.info(
                'Filtering input products from the DIAS catalogue between measurement dates %s and %s' %
                ((DatetimeUtil.toRfc3339(publication_date_min) if publication_date_min else 'None'),
                DatetimeUtil.toRfc3339(publication_date_max)))
            publication_date_min = DatetimeUtil.fromRfc3339(DatetimeUtil.toRfc3339(publication_date_min))
            publication_date_max = DatetimeUtil.fromRfc3339(DatetimeUtil.toRfc3339(publication_date_max))
            fsc_product_list = GfscJob.filter_product_list(fsc_product_large_list,tile_id=tile_id,measurement_date_min=publication_date_min,measurement_date_max=publication_date_max)
            wds_product_list = GfscJob.filter_product_list(wds_product_large_list,tile_id=tile_id,measurement_date_min=publication_date_min,measurement_date_max=publication_date_max)
            sws_product_list = GfscJob.filter_product_list(sws_product_large_list,tile_id=tile_id,measurement_date_min=publication_date_min,measurement_date_max=publication_date_max)
            # GFSC products will be added in just before processing, because they may not be produced yet.
            logger.info('%i input products found.' % len(fsc_product_list + wds_product_list + sws_product_list))
            jobs.append(GfscJob(
                tile_id=tile_id,
                product_date=product_date,
                curation_timestamp=curation_timestamp,
                aggregation_timespan=aggregation_timespan,
                triggering_product_id=product.product_id,
                triggering_product_publication_date=product.publication_date,
                fsc_id_list=[product.product_id for product in fsc_product_list],
                fsc_publication_date_list=[DatetimeUtil.toRfc3339(product.publication_date) for product in fsc_product_list],
                fsc_measurement_date_list=[DatetimeUtil.toRfc3339(product.measurement_date) for product in fsc_product_list],
                wds_id_list=[product.product_id for product in wds_product_list],
                wds_publication_date_list=[DatetimeUtil.toRfc3339(product.publication_date) for product in wds_product_list],
                wds_measurement_date_list=[DatetimeUtil.toRfc3339(product.measurement_date) for product in wds_product_list],
                sws_id_list=[product.product_id for product in sws_product_list],
                sws_publication_date_list=[DatetimeUtil.toRfc3339(product.publication_date) for product in sws_product_list],
                sws_measurement_date_list=[DatetimeUtil.toRfc3339(product.measurement_date) for product in sws_product_list],
                ))
        logger.info('%i jobs added to job list.',len(jobs))

        # Print statistics (for debugging)
        # GfscJobUtil.statistics(jobs, GfscJob, logger)

        # Keep only the jobs focusing on the appropriate tiles
        jobs = [j for j in jobs if j.tile_id[1:] in GfscJob.__TILE_IDS]

        # Keep only the jobs that do not already exist in the database
        jobs = GfscJobUtil.get_unique_jobs(jobs, last_inserted_job, GfscJob,
            internal_database_parallel_request, logger)

        # Keep only one job for each date and tile. Curation times are same.
        # When the system is restarted after a period, there will many unccessary FSC
        # triggerred jobs which will be cancelled. This will also decrease number of
        # jobs if there are both WDS and SWS in the tile for the day
        filtered_jobs = []
        filtered_dates_tiles = []
        # Avoid FSC triggerred so that they dont wait for 3h
        for job in jobs:
            if GfscJobUtil.get_input_product_type(job.triggering_product_id) == 'fsc':
                continue
            if (job.product_date,job.tile_id) not in filtered_dates_tiles:
                filtered_dates_tiles.append((job.product_date,job.tile_id))
                filtered_jobs.append(job)
        # Now include FSC triggered so we dont miss any
        for job in jobs:
            if (job.product_date,job.tile_id) not in filtered_dates_tiles:
                filtered_dates_tiles.append((job.product_date,job.tile_id))
                filtered_jobs.append(job)
        jobs = filtered_jobs

        # Filter reprocessed jobs which have been re-published recently,
        # but with a measurement date older than the operational system
        # processing start date (01/05/2020).
        # TODO [Major] gfsc start date should be (01/07/2021)
        jobs, reprocessed_jobs = GfscJobUtil.filter_reprocessed_input_products(
            jobs, GfscJob.__PROCESSING_START_DATE)
        if len(reprocessed_jobs) > 0:
            logger.info('%i of the jobs are reprocessing jobs.',len(reprocessed_jobs))

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
                StoredProcedure.get_gfsc_jobs_with_status_product_date_tile(
                    [JobStatus(x) for x in list(range(1,17))],
                    job.product_date.strftime('%Y-%m-%dT%H:%M:%S'),
                    job.tile_id,
                    GfscJob(),
                    logger_func=logger_func,
                    set_timeout=False
                )
            )
        return existing_jobs


    @staticmethod
    def configure_batch_jobs(jobs, logger_func):
        '''

        :param jobs: list of jobs to be configured.
        :param logger_func: Logger instance
        '''
        # TODO [Minor] this function may remain empty and the comment bellow could be remove

        return jobs
