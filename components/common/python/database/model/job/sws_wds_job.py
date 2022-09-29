import os
import re
import json
import copy
from datetime import datetime, timedelta
from enum import Enum
from yaml import safe_load as yaml_load

from .job_template import JobTemplate
from .job_status import JobStatus
from .worker_flavors import WorkerFlavors
from .system_parameters import SystemPrameters
from ....util.datetime_util import DatetimeUtil
from ....util.eea39_util import Eea39Util
from ....util.hrsi_util import HrsiUtil
from ....util.sys_util import SysUtil
from ....util.creodias_util import CreodiasUtil
from ....util.exceptions import CsiInternalError
from ...rest.stored_procedure import StoredProcedure
from ....util.resource_util import ResourceUtil
from .sws_wds_assembly_status import AssemblyStatus
from ....util.sws_wds_job_util import SwsWdsJobUtil
from ....util.sws_wds_sentinel1_assembly import s1_product_assembly, intersectFscS1


class SwsWdsJob(JobTemplate):
    '''
    Description of a job for the generation of one SAR Wet Snow (SWS) product from Sentinel-1 GRD product.

    :param nrt: (boolean) Near Real-Time context ? If false: archive reprocessing.
    :param infos: (json) information relative to SWS product.
    :param path: SWS product path in the DIAS
    :param measurement_date: (datetime) Datatake sensing start time
    :param s1_sensing_time: (datetime) sensing time from MTD_TL.xml
    :param s1_esa_creation_latest_date: (datetime) L1C creation date by ESA.
    :param s1_esa_publication_latest_date: (datetime) L1C publication date in the ESA hub.
    :param s1_dias_publication_latest_date: (datetime) L1C publication date in the DIAS catalogue.
    :param completion_date: (datetime) SWS publication date in the bucket.
    :param json_publication_date: (datetime) SWS JSON publication date on RabbitMQ endpoint.
    :param backward_reprocessing_run: (boolean) Notify if a Backward reprocessing should (False)
    or has already (True) be run on this job.
    :param reprocessing_context: (string) Notify the reprocessing context for a non-NRT job
    '''

    # Database table name
    __TABLE_NAME = "sws_wds_jobs"

    # Class job name
    JOB_NAME = "SWS/WDS"

    # Sentinel input product type
    INPUT_PRODUCT_TYPE = "s1"

    # Name of the Nomad job processing the present job type
    NOMAD_JOB_NAME = "ws-processing"

    # Worker flavor required for the present job type processing
    WORKER_FLAVOR_NAME = WorkerFlavors.extra_large.value

    # Name of the products the job will generate during it's processing
    OUTPUT_PRODUCTS_LIST = ["sws", "wds"]

    # L2A data bucket's name ????
    __SIP_DATA_BUCKET = SysUtil.read_env_var("CSI_SIP_DATA_BUCKET")

    # Restrict the orchestrator to a specific tile ID.
    # __TILE_RESTRICTION = ['30TYN', '32TLR', '32TMS', '33WWR', '33WXS', '34WBD', 
    #                       '32TLS', '32TLT', '38SLJ', '38SMJ', '38TMK']
    __TILE_RESTRICTION = None

    # List of all the EEA39 Sentinel-2 tile IDs or filtered accordint __TILE_RESTRICTION.
    __TILE_IDS = Eea39Util.get_tiles(tile_restriction=__TILE_RESTRICTION)

    # EEA39 Area Of Interest (AOI) to request, in WGS84 projection.
    __GEOMETRY = Eea39Util.get_geometry(tile_restriction=__TILE_RESTRICTION)

    # Get the tracks per tile for processing
    __tiles_tracks = SwsWdsJobUtil.get_tiles_tracks_config()

    # Get the mountain tiles
    __tiles_mountains = SwsWdsJobUtil.get_tiles_mountains_config()

    # Max number of pages to request (if None: infinite).
    __MAX_REQUESTED_PAGES = None if __TILE_RESTRICTION is None else 20

    # Keep track of the last SWS job inserted in the database.
    LAST_INSERTED_JOB = None

    # Time after the input product acquisition within which we replace any job
    # already generated from this input product.
    DUPLICATE_INPUT_PRODUCT_VALID_TIME = timedelta(days=1)

    # Operational system processing start date (01/05/2020)
    __PROCESSING_START_DATE = datetime(2021, 5, 1)

    # Name of the stored procedure used to retrieve SwsWdsJobs with a given status
    GET_JOBS_WITH_STATUS_PROCEDURE_NAME = "sws_wds_jobs_with_last_status"

    LAST_S1_PRODUCT_PUBLISHED = None
    LAST_FSC_PRODUCT_PUBLISHED = None

    def __init__(self, **kwds):
        self.nrt = None
        self.sws_infos = None
        self.sws_path = None
        self.sws_completion_date = None
        self.sws_json_publication_date = None
        self.wds_infos = None
        self.wds_path = None
        self.wds_completion_date = None
        self.wds_json_publication_date = None
        self.measurement_date = None
        # self.backward_reprocessing_run = None
        # self.reprocessing_context = None
        self.s1_id_list = None
        self.s1_path_list = None
        self.s1_esa_publication_latest_date = None
        self.s1_dias_publication_latest_date = None
        self.assembly_id = None                         # uniq ID    s1_assembly_PLATFORM_MISSIONTAKEID_EPSG_NNN
        self.assembly_status = None
        self.assembly_params = None                     # json : s1_mission_take_id, s1_footprint, s1_sensing_time, s1_relOrbit, s1_platform
        self.assembly_path = None
        self.assembly_return_code = None
        self.assembly_reference_job = None              # up to now not really used
        self.assembly_master_job_id = None
        self.fsc_id_list = None
        self.fsc_path_list = None
        self.fsc_creation_latest_date = None
        self.fsc_publication_latest_date = None

        # Call the parent constructor AFTER all the attributes are initialized with None
        super().__init__(SwsWdsJob.__TABLE_NAME)

        # Attribute values given by the caller
        for key, value in kwds.items():
            setattr(self, key, value)

    ################################################################
    # Start of specific methods

    def from_database_value(self, attribute, value):
        '''Parse a string value as it was inserted in the database.'''

        if attribute.endswith('_date'):
            return DatetimeUtil.fromRfc3339(value)
        elif attribute.endswith('_list'):
            return value.split(';')
        elif attribute == 'assembly_params':
            return json.loads(value)
        elif attribute == 'sws_infos':
            return json.loads(value)
        elif attribute == 'wds_infos':
            return json.loads(value)

        # Default: call parent class
        return super().from_database_value(attribute, value)

    def to_database_value(self, attribute, value):
        '''Return a value as it must be inserted in the database.'''

        # Return enum name
        if isinstance(value, Enum):
            return value.name

        # Convert datetimes to string
        elif isinstance(value, datetime):
            return DatetimeUtil.toRfc3339(value)

        # Convert dictionaries to json
        elif isinstance(value, dict):
            return json.dumps(value)

        # Convert dictionaries to json
        elif isinstance(value, list):
            return ";".join(value)

        # Default: call parent class
        return super().to_database_value(attribute, value)

    def get_last_inserted_job(self, attribute_name='s1_dias_publication_latest_date'):
        '''
        Return the last job that has been inserted in the database, based on
        the highest XXX DIAS publication date.
        '''

        return super().select(attribute_name).max(attribute_name)

    def get_input_product_dias_publication_date(self):
        '''Return the S1 DIAS publication date.'''

        return self.s1_dias_publication_latest_date

    def get_input_product_esa_creation_date(self):
        '''Return the Sentinel-1 GRD ESA creation date.'''

        return self.s1_esa_creation_latest_date

    def get_input_product_esa_publication_date(self):
        '''Return the Sentinel-1 GRD ESA publication date.'''

        return self.s1_esa_publication_latest_date

    def set_input_product_esa_publication_date(self, value):
        '''Set the Sentinel-1 ESA creation date.'''

        self.s1_esa_publication_latest_date = value

    def get_input_product_id(self):
        '''Return Sentinel-1 GRD IDs.'''

        return self.s1_id_list

    def set_input_product_reference_job(self, value: bool):
        '''
        Set the Assembly reference job parameter value.

        :param value: boolean value to notify if the job is a reference for a given S1-GRD.
        '''

        self.assembly_reference_job = value

    def set_product_publication_date(self, product_name: str = None, publication_json: dict = None):
        '''
        Set the publication date of a given type of product.

        :param product_name: name of the product to update the publication date.
            This parameter is not mandatory, but can be used if several products
            are generated by the same job to distinguish which product info should
            be updated.
        '''

        now = datetime.utcnow()
        if product_name.lower() == "sws":
            self.sws_json_publication_date = now
            if publication_json is not None:
                self.sws_infos = publication_json
        elif product_name.lower() == "wds":
            self.wds_json_publication_date = now
            if publication_json is not None:
                self.wds_infos = publication_json
        else:
            raise CsiInternalError(
                "Error",
                "unknown product_name: " + str(product_name)
            )

    def generated_a_product(self):
        '''Return a boolean to notify if the job did generate a product or not.'''

        return (self.sws_path is not None) or (self.wds_path is not None)

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
        else:
            # Always a near real-time context
            self.nrt = True

            # Set Assembly status
            self.assembly_status = AssemblyStatus.pending.name

    def configure_single_job(self, logger):
        '''
        Perform job specific configuration.

        :param logger: Logger instance
        '''

        # Set up output message
        message = None
        
        # default
        status_to_set = JobStatus.configured

        assembly_master_job_id = self.assembly_master_job_id

        if assembly_master_job_id > 0:
            master_jobs = SwsWdsJob().id_eq(assembly_master_job_id).get()
            if len(master_jobs) == 0:
                logger.error(
                    f"Couldn't retrieve last status for master job job with id \
                    '{assembly_master_job_id}' for job with id {self.id}"
                )
                #TODO relieve error
                raise CsiInternalError(
                    "Job Configuration Error",
                    "Couldn't retrieve master job."
                )

            master_job = master_jobs[-1]

            # Compute job status function of the job dependencies
            status_to_set = self.update_job_status_function_of_dependencies(
                master_job,
                logger
            )
            if status_to_set == JobStatus.ready:
                # master has successfully generated the Assembly file
                # Set the Assembly file to the master job
                self.assembly_path = master_job.assembly_path
        elif assembly_master_job_id == 0:
            # myself is the master
            status_to_set = JobStatus.ready
        else:
            status_to_set = JobStatus.configured

        return self, status_to_set, message

    def update_job_status_function_of_dependencies(self, master_job, logger):
        '''
        Update jobs status function of their dependencies :
           - if job is independent -> set status to ready (wait for execution)
           - if job is dependent on an other job which status is processed or higher,
               -> set status to ready (wait for execution)
           - if job is dependent on an other job which status is not at least processed,
               or failed -> set status to configured (wait for dependency completion)

        :param job: single job for which we want to compute the status to be set.
        :param master_job: master job of the current.
        :param logger: Logger instance
        '''

        # Check if the dependent job has completed its processing yet
        if master_job:

            # master job status prior to 'processed',
            # dependency is not satisfied, so job status is set to 'configured'
            if (isinstance(master_job.last_status_id, int)
               and master_job.last_status_id < JobStatus.processed.value):

                logger.debug(
                    "Job '%d' is waiting for job '%d' to be completed!"
                    % (self.id, master_job.id)
                )
                return JobStatus.configured

            # Raise an error if the master job status is not/not properly set
            elif not isinstance(master_job.last_status_id, int):
                logger.error(
                    "Couldn't retrieve last status for master job with id '%d'"
                    % master_job.id
                )
                raise CsiInternalError(
                    "Job Configuration Error",
                    "Couldn't retrieve last status for master job."
                )

            # Dependency is satisfied so we check dependency's output product status
            else:
                assembly_status = AssemblyStatus[master_job.assembly_status]
                # If dependency's output product has been generated, set status to 'ready'
                if assembly_status == AssemblyStatus.generated:
                    return JobStatus.ready
                elif assembly_status == AssemblyStatus.empty:
                    # if the S1 Assembly file is empty, the tile will be also empty
                    return JobStatus.cancelled
                # NOTE: for now we cancel if master AssemblyStatus is cancelled or deleted
                elif assembly_status.value >= AssemblyStatus.generation_aborted.value:
                    # the S1 Assembly file is not done
                    return JobStatus.cancelled

                return JobStatus.configured

        # No dependency so we set status to 'ready'
        return JobStatus.ready

    def get_products_publication_jsons(self, publication_json_template: dict):
        '''
        Fill the JSONs to be sent to notify product publication for each product
        generated by this SWS WDS job.

        :param dict_notifying_publication: JSON template to be filled before sending.
        '''

        products_publication_jsons = []

        # Iterate over each type of generated product
        for product in self.OUTPUT_PRODUCTS_LIST:

            # Create a new instance of the JSON template for each iteration
            dict_notifying_publication = copy.deepcopy(publication_json_template)

            # Json info set by ws_software/worker
            json_set_by_worker = None
            # Date set by worker when sending each product in the bucket
            product_generation_date = None

            if product == "sws":
                # If product info were set by the worker, load them
                if self.sws_infos is not None and self.sws_path is not None:
                    json_set_by_worker = self.sws_infos

                    # Retrieve parameters values specific to product type
                    if self.sws_completion_date is not None:
                        product_generation_date = self.sws_completion_date.strftime(
                            '%Y-%m-%dT%H:%M:%S.%f')

                    (s3_bucket, object_path) = SwsWdsJobUtil.split_bucket_and_object_from_path(
                        self.sws_path, prefix=True)

                    product_identifier = self.sws_path

                    thumbnail = os.path.join(
                        SwsWdsJobUtil.get_quicklook_bucket_path(object_path),
                        "thumbnail.png"
                    )
                else:
                    continue

            elif product == "wds":
                # If product info were set by the worker, load them
                if self.wds_infos is not None and self.wds_path is not None:
                    json_set_by_worker = self.wds_infos

                    # Retrieve parameters values specific to product type
                    if self.wds_completion_date is not None:
                        product_generation_date = self.wds_completion_date.strftime(
                            '%Y-%m-%dT%H:%M:%S.%f')

                    (s3_bucket, object_path) = SwsWdsJobUtil.split_bucket_and_object_from_path(
                        self.wds_path, prefix=True)

                    product_identifier = self.wds_path

                    thumbnail = os.path.join(
                        SwsWdsJobUtil.get_quicklook_bucket_path(object_path),
                        "thumbnail.png"
                    )
                else:
                    continue
            else:
                raise CsiInternalError(
                    "Error",
                    "unknown product: " + str(product)
                )

            # Product info were not set by worker -> raise a warning
            if json_set_by_worker is None:
                self.logger.warning("Couldn't publish job with id '%s' '%s' "
                                    "product's JSON as no information were set by the worker!"
                                    % (self.id, product))
                continue

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

            except KeyError:
                self.logger.error("Couldn't publish job with id '%s' '%s' "
                                  "product's JSON as information set by worker are not "
                                  "relevant!" % (self.id, product))
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
                            "startDate"] = self.measurement_date.strftime(
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
                            "mission"] = SwsWdsJob.INPUT_PRODUCT_TYPE.upper()

            except Exception as error:
                self.logger.error(f"Couldn't publish job with id '{self.id}' \
                                '{product}' product's JSON as the following \
                                error occured during its setting : \n{error}")
                continue

            products_publication_jsons.append(dict_notifying_publication)

        return products_publication_jsons

    ################################################################
    # Start of static specifics methods

    @staticmethod
    def get_input_product_search_default_duration_in_days(logger=None):
        '''
        return the S1, FSC default search duration (in days) stored in the system parameters.
        :param logger: logger.debug or logger.info or ...
        '''
        return SystemPrameters().get(logger).s1_search_default_duration_in_days

    @staticmethod
    def get_jobs_to_create(internal_database_parallel_request: int, logger):
        '''
        Request the DIAS for new Sentinel-1 GRD products and determine the list of jobs
        which should be created from it. We filter and order this list.

        :param internal_database_parallel_request: number of requests which can
            be performed in parallel to the database.
        :param logger: Logger instance.
        '''

        tiles_mountains = SwsWdsJob.__tiles_mountains
        tiles_tracks = SwsWdsJob.__tiles_tracks

        # timeliness="Fast-24h" , "NRT-3h"
        s1_other_params = {
            "polarisation": "VV VH",
            "sensorMode": "IW"
        }

        # TODO [debug] : remove the increased backward search
        search_default_duration = SwsWdsJob.get_input_product_search_default_duration_in_days(logger.debug)
        date_now = datetime.utcnow().replace(tzinfo=None)
        date_max = date_now.replace(microsecond=0)

        if SwsWdsJob.LAST_INSERTED_JOB is not None:
            if SwsWdsJob.LAST_INSERTED_JOB.s1_dias_publication_latest_date is not None:
                SwsWdsJob.LAST_S1_PRODUCT_PUBLISHED = SwsWdsJob.LAST_INSERTED_JOB.s1_dias_publication_latest_date.replace(tzinfo=None)

            if SwsWdsJob.LAST_INSERTED_JOB.fsc_publication_latest_date is not None:
                SwsWdsJob.LAST_FSC_PRODUCT_PUBLISHED = SwsWdsJob.LAST_INSERTED_JOB.fsc_publication_latest_date.replace(tzinfo=None)

        if not SwsWdsJob.LAST_S1_PRODUCT_PUBLISHED:
            last_inserted_job_with_s1_dias_date = SwsWdsJob().get_last_inserted_job().get(logger_func=logger.debug)
            if (len(last_inserted_job_with_s1_dias_date) > 0 
            and last_inserted_job_with_s1_dias_date[0].s1_dias_publication_latest_date is not None):
                SwsWdsJob.LAST_S1_PRODUCT_PUBLISHED = last_inserted_job_with_s1_dias_date[0].s1_dias_publication_latest_date.replace(tzinfo=None)
            else:
                SwsWdsJob.LAST_S1_PRODUCT_PUBLISHED = date_max - timedelta(days=search_default_duration)

        if not SwsWdsJob.LAST_FSC_PRODUCT_PUBLISHED:
            last_inserted_job_with_fsc_publi_date = StoredProcedure.get_last_job_with_fsc_publication_latest_date(SwsWdsJob(), logger.debug)
            if (len(last_inserted_job_with_fsc_publi_date) > 0 
            and last_inserted_job_with_fsc_publi_date[0].fsc_publication_latest_date is not None):
                SwsWdsJob.LAST_FSC_PRODUCT_PUBLISHED = last_inserted_job_with_fsc_publi_date[0].fsc_publication_latest_date.replace(tzinfo=None)
            else:
                SwsWdsJob.LAST_FSC_PRODUCT_PUBLISHED = date_max - timedelta(days=search_default_duration)

        logger.debug("SwsWdsJob.LAST_FSC_PRODUCT_PUBLISHED: " + str(SwsWdsJob.LAST_FSC_PRODUCT_PUBLISHED))
        logger.debug("SwsWdsJob.LAST_S1_PRODUCT_PUBLISHED: " + str(SwsWdsJob.LAST_S1_PRODUCT_PUBLISHED))

        date_min = SwsWdsJob.LAST_FSC_PRODUCT_PUBLISHED
        max_requested_pages = SwsWdsJob.__MAX_REQUESTED_PAGES if date_min is None else None

        logger.info(
            'Request new FSC products from the HR-S&I catalogue between dates %s and %s' %
            ((DatetimeUtil.toRfc3339(date_min) if date_min else 'None'), DatetimeUtil.toRfc3339(date_max))
        )

        # query for fsc products
        fsc_products_within_date = HrsiUtil().request(
            logger,
            'fsc',
            SwsWdsJob.__GEOMETRY,
            date_min,
            date_max,
            # TODO [SHOULD] : remove the request size limiter once the backward late 
            # triggered jobs won't interfer with the API requests anymore
            # max_requested_pages=max_requested_pages)
            max_requested_pages=max_requested_pages,
            other_params={'startDate':f"{DatetimeUtil.toRfc3339(date_min - timedelta(days=30))}", 
                'completionDate':f"{DatetimeUtil.toRfc3339(date_max)}"})

        if fsc_products_within_date:
            date_min_fsc_product_measurement = date_max
            for fsc_product in fsc_products_within_date:
                # remove 'T' prefix in tile_id, the tile_id in the original KML is without 'T'
                if fsc_product.tile_id[0] == 'T':
                    fsc_product.tile_id = fsc_product.tile_id[1:]
                fsc_product.measurement_date = fsc_product.measurement_date.replace(tzinfo=None)
                if date_min_fsc_product_measurement > fsc_product.measurement_date:
                    date_min_fsc_product_measurement = fsc_product.measurement_date

            # we search for all the same day for a fsc
            # the publication is for sure after measurement
            date_min_measured_fsc_product = datetime(date_min_fsc_product_measurement.year, date_min_fsc_product_measurement.month, date_min_fsc_product_measurement.day)
            date_min_published_s1_product = min(SwsWdsJob.LAST_S1_PRODUCT_PUBLISHED, date_min_measured_fsc_product)
        else:
            date_min_published_s1_product = SwsWdsJob.LAST_S1_PRODUCT_PUBLISHED

        logger.debug("Number of new FSC Products %d" % len(fsc_products_within_date))
        logger.info(
            'Request new S1 products from the DIAS catalogue between dates %s and %s' %
            ((DatetimeUtil.toRfc3339(date_min_published_s1_product) if date_min_published_s1_product else 'None'),
                DatetimeUtil.toRfc3339(date_max))
        )

        # Request new input products in the DIAS catalogue
        s1_product_list = CreodiasUtil().request(
            logger,
            SwsWdsJob.INPUT_PRODUCT_TYPE,
            SwsWdsJob.__GEOMETRY,
            date_min_published_s1_product,
            date_max,
            other_params=s1_other_params,
            max_requested_pages=max_requested_pages,
            get_manifest=True)

        logger.debug("Number of S1 Products: %d" % len(s1_product_list))

        # after external request, which could raise an error
        # SwsWdsJob.LAST_S1_PRODUCT_PUBLISHED = date_max
        # SwsWdsJob.LAST_FSC_PRODUCT_PUBLISHED = date_max

        if len(s1_product_list) == 0:
            # nothing todo
            return [], []

        date_min_measured = min([s1.sentinel1_id.start_time for s1 in s1_product_list])

        # query DB for all jobs we already have, there can be any combination
        jobs_within_date = StoredProcedure.get_jobs_within_measurement_date(
            SwsWdsJob(),
            "measurement_date",
            date_min_measured,
            date_max,
            logger)

        # Keep only the tiles of mountain and fsc products
        req_tiles = tiles_mountains + [fsc_product.tile_id for fsc_product in fsc_products_within_date]
        req_tiles = list(set(req_tiles))

        # Keep only the tiles focusing on the appropriate tiles
        if SwsWdsJob.__TILE_RESTRICTION:
            req_tiles = [t for t in req_tiles if t in SwsWdsJob.__TILE_IDS]

        # get the s1 intersected tiles with assembly info
        proc_tiles_all, _, proc_assembly_info = s1_product_assembly(s1_product_list, req_tiles, logger=logger)

        if len(proc_tiles_all) == 0:
            # nothing todo
            return [], []

        jobnames_sws = [j.name for j in jobs_within_date if j.assembly_params['generate_sws_product']]
        jobnames_wds = [j.name for j in jobs_within_date if j.assembly_params['generate_wds_product']]

        jobs = []
        for tile in proc_tiles_all:
            new_tile_sws = False
            job_name = tile['tile_id'] + "_" + tile['missionTakeId'] + "_" + tile['platform']
            job_name = job_name.upper()
            if (tile['tile_id'] in tiles_mountains) and (job_name not in jobnames_sws):
                jobnames_sws.append(job_name)
                new_tile_sws = True

            fsc_product_in_tile = []
            for fsc_product in fsc_products_within_date:
                if (tile['tile_id'] == fsc_product.tile_id and
                   tile['sourceProduct_startTime'].strftime("%Y%m%d") == fsc_product.measurement_date.strftime("%Y%m%d")):
                    if job_name not in jobnames_wds:
                        jobnames_wds.append(job_name)
                        fsc_product_in_tile.append(fsc_product)

            new_tile_wds = len(fsc_product_in_tile) > 0
            if new_tile_sws or new_tile_wds:
                if tile['relativeOrbitNumber'] not in tiles_tracks[tile['tile_id']]:
                    # skip tiles without reference or auxiliary files for the track
                    logger.debug(f"missing ref/aux data for tile: {tile['tile_id']} and track: {tile['relativeOrbitNumber']}")
                    continue
                # Create a SwsWdsJob for each new tile
                if new_tile_wds:
                    intersectfscs1 = intersectFscS1([i.gml_geometry for i in fsc_product_in_tile], tile['sourceGeometry'])
                    wds_infos = {"resto": {"geometry": {"wkt": intersectfscs1}}}
                else:
                    wds_infos = None
                if new_tile_sws:
                    sws_infos = {"resto": {"geometry": {"wkt": tile['sourceGeometry']}}}
                else:
                    sws_infos = None
                assemblyId = tile['assemblyId']
                assembly_params = copy.deepcopy(proc_assembly_info[assemblyId])
                assembly_params['relativeOrbitNumber'] = tile['relativeOrbitNumber']
                assembly_params['missionTakeId'] = tile['missionTakeId']
                assembly_params['platform'] = tile['platform']
                assembly_params['areaUTM'] = tile['areaUTM']
                assembly_params['envelope'] = ";".join([str(i) for i in tile['tile_envelope']])
                assembly_params['sourceGeometry'] = tile['sourceGeometry']
                assembly_params['sourceProduct_startTime'] = tile['sourceProduct_startTime'].isoformat()
                assembly_params['sourceProduct_stopTime'] = tile['sourceProduct_stopTime'].isoformat()
                assembly_params['generate_sws_product'] = new_tile_sws
                assembly_params['generate_wds_product'] = new_tile_wds
                job = SwsWdsJob(
                    tile_id=tile['tile_id'],
                    name=job_name,
                    measurement_date=tile['sourceProduct_startTime'],
                    wds_infos=wds_infos,
                    sws_infos=sws_infos,
                    s1_id_list=assembly_params['s1_product_id'],
                    s1_path_list=assembly_params['s1_productIdentifier'],
                    s1_dias_publication_latest_date=tile['sourceProduct_published'],
                    assembly_id=assemblyId,
                    assembly_master_job_id=-1,                                          # flag used to decide in update_jobs_for_assembly_master
                    assembly_params=assembly_params,
                    fsc_id_list=[i.product_id for i in fsc_product_in_tile] if new_tile_wds else None,
                    fsc_path_list=[i.product_path for i in fsc_product_in_tile] if new_tile_wds else None,
                    fsc_creation_latest_date=max([i.creation_date for i in fsc_product_in_tile]) if new_tile_wds else None,
                    fsc_publication_latest_date=max([i.publication_date for i in fsc_product_in_tile]) if new_tile_wds else None
                )

                jobs.append(job)

        # Print statistics (for debugging)
        # SwsWdsJobUtil.statistics(jobs, SwsWdsJob, logger)

        # We have only the jobs that do not already exist in the database and appropriate tiles

        # Filter reprocessed jobs which have been re-published recently,
        # but with a measurement date older than the operational system
        # processing start date (01/05/2020).
        s1_processing_start_date = SystemPrameters().get(logger).s1_processing_start_date
        s1_processing_start_date = datetime.strptime(s1_processing_start_date, "%Y-%m-%dT%H:%M:%S")

        jobs, reprocessed_jobs = SwsWdsJobUtil.filter_reprocessed_input_products(
            jobs, s1_processing_start_date)

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
    def get_jobs_without_esa_publication_date(logger_func):
        '''
        Retrieve all the SWS jobs inserted in the database which have
        no esa publication date set.

        :param logger_func: logger.debug or logger.info or ...
        '''

        return SwsWdsJob().attribute_is(
            's1_esa_publication_latest_date', 'null').get(logger_func)

    @staticmethod
    def configure_batch_jobs(jobs, logger_func):
        '''
        Perform L1C specific configuration, required for job processing.
        Here we set the ESA publication date, compute the job priority, and fix
        the ESA publication date for already processed jobs which would not have
        this information set.

        :param jobs: list of SWS jobs to be configured.
        :param logger_func: Logger instance
        '''

        if len(jobs) == 0:
            return jobs

        # Update the jobs with information found in the ESA hub.
        jobs = SwsWdsJobUtil.update_jobs_with_esa_info(jobs, logger_func)

        # Update the jobs with the master assembly job.
        jobs = SwsWdsJobUtil.update_jobs_for_assembly_master(jobs, SwsWdsJob(), logger_func)

        # Update the job priority levels
        jobs = SwsWdsJobUtil.update_priority(jobs)

        # Fix ESA publication date for already configured jobs which are
        # lacking this parameter.
        SwsWdsJob.fix_esa_publication_date(logger_func)

        return jobs

    @staticmethod
    def fix_esa_publication_date(logger):
        '''
        Set ESA publication date for existing SWS jobs, which have
        already been configured but are lacking this parameter.

        :param job_type: specific type of job on which we want to ensure that the esa
            publication date are set for each job of this type present in the database.
        '''

        # Find all jobs which have no ESA publication date set
        jobs = SwsWdsJob.get_jobs_without_esa_publication_date(logger_func=logger.debug)

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

        logger.info('Update the ESA publication date of %d configured %s jobs' % (
            len(jobs), SwsWdsJob.JOB_NAME))

        # Update the jobs with information found in the ESA hub, but do not change the job status.
        jobs = SwsWdsJobUtil.update_jobs_with_esa_info(jobs, logger, status_update=False)

        for job in jobs:
            job.patch(patch_foreign=True, logger_func=logger.debug)
