import re
import json
from datetime import datetime, timedelta
from shapely.geometry import Polygon

from .job_template import JobTemplate
from .job_status import JobStatus
from .worker_flavors import WorkerFlavors
from ....util.datetime_util import DatetimeUtil
from ....util.eea39_util import Eea39Util
from ....util.creodias_util import CreodiasUtil
from ....util.esa_util import EsaUtil
from ...rest.stored_procedure import StoredProcedure


class RlieS1Job(JobTemplate):
    '''
    Description of a generic test job, used for integration validation purpose.
    '''
    
    ################################################################
    # Start of specific properties
    
    # Database table name
    __TABLE_NAME = "rlies1_jobs"

    # Class job name
    JOB_NAME = "rlies1"

    # Sentinel input product type
    INPUT_PRODUCT_TYPE = "s1"

    # Name of the Nomad job processing the present job type
    NOMAD_JOB_NAME = "rlies1-processing"

    # Worker flavor required for the present job type processing
    WORKER_FLAVOR_NAME = WorkerFlavors.large.value

    # Name of the products the job will generate during it's processing
    OUTPUT_PRODUCTS_LIST = ["rlies1"]

    # Restrict the orchestrator to a specific tile ID.
    # __TILE_RESTRICTION = ['30TYN', '32TLR', '32TMS', '33WWR', '33WXS', '34WBD', 
    #                       '32TLS', '32TLT', '38SLJ', '38SMJ', '38TMK']
    __TILE_RESTRICTION = None

    # List of all the EEA39 Sentinel-2 tile IDs or filtered accordint __TILE_RESTRICTION.
    __TILE_IDS = Eea39Util.get_tiles(tile_restriction=__TILE_RESTRICTION)

    # EEA39 Area Of Interest (AOI) to request, in WGS84 projection.
    __GEOMETRY = Eea39Util.get_geometry(tile_restriction=__TILE_RESTRICTION)

    # Keep track of the last FSC RLIE job inserted in the database.
    LAST_INSERTED_JOB = None

    # Name of the stored procedure used to retrieve TestJob with a given status
    GET_JOBS_WITH_STATUS_PROCEDURE_NAME = "rlies1_jobs_with_last_status"
    
    # Operational system processing start date (01/05/2020)
    __PROCESSING_START_DATE = datetime(2021, 4, 1)

    ################################################################
    # End of specific properties


    def __init__(self, **kwds):
        
        #filled in job creation
        self.s1grd_id = None
        self.product_path = None
        self.measurement_date_start = None
        self.measurement_date_end = None
        self.s1grd_dias_publication_date = None
        self.s1grd_esa_publication_date = None
        self.s2tile_ids_json = None #sorted S2 tile IDs. Intersection computed from Wekeo S1 GRD geometries. TBD: check if those geometries are accurate enough and take into account S1 GRD area of definition.
        self.reprocessing_context = None #TBD
        
        #filled in job configuration
        
        #filled in worker after job processing
        self.rlies1_products_completion_date = None
        self.rlies1_product_paths_json = None
        self.rlies1_product_json_submitted_json = None #is partially filled in worker and later completed in post-processing / publication stage
        
        #filled in post-processing / publication stage (get_products_publication_jsons)
        self.rlies1_products_publication_date = None

        
        # Call the parent constructor AFTER all the attributes are initialized with None
        super().__init__(self.__TABLE_NAME)

        # Attribute values given by the caller
        for key, value in kwds.items():
            setattr(self, key, value)


    def from_database_value(self, attribute, value):
        '''Parse a string value as it was inserted in the database.'''

        if attribute.split('_')[-1] == 'json':
            return json.loads(value)
            
        if isinstance(value, str) and ('_date' in attribute):
            return DatetimeUtil.fromRfc3339(value)

        # Default: call parent class
        return super().from_database_value(attribute, value)

    ################################################################
    # Start of specific methods

    def get_last_inserted_job(self):
        '''
        Return the last job that has been inserted in the database.
        Note that this method is only usefull for jobs which are requesting external 
        APIs for new products, to ensure to not miss any data between two separated 
        requests. This function is called in the "compute_dias_request_time_range()" 
        method, if you don't need to request any external APIs, you don't need to define it.
        '''
        
        attribute_name = 's1grd_dias_publication_date'
        return super().select(attribute_name).max(attribute_name)


    def job_pre_insertion_setup(self, reprocessed_job=False):
        '''
        Perform set up actions for a given job, before its insertion in database.

        :param reprocessed_job: boolean notifying if the job is an old job part 
            of a reprocessing campaign.
        '''

        return


    def configure_single_job(self, logger_func):
        '''
        Perform input product specific configuration, on a single job, required 
        for its processing.

        :param logger_func: Logger instance.
        '''

        return self, JobStatus.ready, None


    def get_products_publication_jsons(self, publication_json_template: dict):
        '''
        Fill the json to be sent to notify product publication.

        :param publication_json_template: JSON template to be filled before sending.
        '''
        
        assert self.rlies1_product_json_submitted_json is not None, 'rlies1_product_json_submitted_json must be initialised'
        
        return list(self.rlies1_product_json_submitted_json.values())


    def get_input_product_id(self):
        '''Return Sentinel-1 GRD ID.'''

        return self.s1grd_id


    def generated_a_product(self):
        '''Return a boolean to notify if the job did generate a product or not.'''

        if self.rlies1_product_paths_json is not None:
            if len(self.rlies1_product_paths_json) > 0:
                return True
        return False


    def set_input_product_esa_publication_date(self, value):
        '''Set the Sentinel-1 ESA creation date.'''

        self.s1grd_esa_publication_date = value


    def set_product_publication_date(self, product_name: str, publication_json: dict = None):
        '''
        Set the publication date of a given type of product.

        :param product_name: name of the product to update the publication date.
        '''

        self.rlies1_products_publication_date = datetime.utcnow()


    ################################################################
    # Start of static specifics methods

    @staticmethod
    def get_jobs_to_create(internal_database_parallel_request: int, logger):
        '''
        Request an API for new input products and determine the list of jobs 
        which should be created from it. This list of job can be filtered and ordered.
        
        :param internal_database_parallel_request: number of requests which can 
            be performed in parallel to the database.
        :param logger: Logger instance.
        '''

        #get search window :
        #search_date_start: last job DIAS publication date (-1hour margin) if it exists or rlies1_reference_start_date
        #search_date_end: now
        
        # ~ rlies1_reference_start_date = RlieS1Job.__PROCESSING_START_DATE
        # rlies1_reference_start_date = datetime.utcnow() - timedelta(1) #for development purposes, set rlies1_reference_start_date to 1 day prior to now get system DB reference date
        # TODO [Minor] : Rmove code added for Part2 operational phase start
        start_day = datetime.utcnow() - timedelta(1)
        start_day = datetime(start_day.year, start_day.month, start_day.day) #set date to beginning of day at 00:00:00
        rlies1_reference_start_date = start_day

        if RlieS1Job.LAST_INSERTED_JOB:
            last_inserted_job_list = [RlieS1Job.LAST_INSERTED_JOB]
        else:
            last_inserted_job_list = RlieS1Job().get_last_inserted_job().get(logger_func=logger.debug)

        if isinstance(last_inserted_job_list, list) and len(last_inserted_job_list) > 0:
            last_inserted_job = last_inserted_job_list[0]
            last_job_dias_publication_date = last_inserted_job.s1grd_dias_publication_date
        else:
            last_job_dias_publication_date = None

        if last_job_dias_publication_date is not None:
            search_date_start = last_job_dias_publication_date
        else:
            search_date_start = rlies1_reference_start_date
        search_date_end = datetime.utcnow()
        
        #get S2 tile geometry
        s2_tile_geometry_dict = {row['Name']: row['geometry'] for _, row in Eea39Util.read_shapefile().iterrows()}

            
        ################
        #search using CreodiasUtil
        dico_s1_search = CreodiasUtil().request(logger, 's1', RlieS1Job.__GEOMETRY, search_date_start, search_date_end, other_params={"polarisation": "VV VH", "sensorMode": "IW"}, get_manifest=False)
        dico_s1_search = {el.product_id: el for el in dico_s1_search if '_'.join(el.product_id.split('_')[1:3]) == 'IW_GRDH'}
        logger.info('%d S1 products identified'%len(dico_s1_search))
        
        #eliminate all S1 products that are already referenced in database (should not happen, just some additional security in case of future human programming error)
        #DATABASE : get all S1 products within search_date_start and search_date_end
        s1_product_database_set = set([job_loc.s1grd_id for job_loc in StoredProcedure.get_jobs_within_measurement_date(RlieS1Job(), 's1grd_dias_publication_date', \
            search_date_start, search_date_end, logger.info)])
        logger.info('%d S1 products already in DB in search window'%len(s1_product_database_set))
        dico_s1_search = {key: dico_s1_search[key] for key in dico_s1_search if key not in s1_product_database_set}
        logger.info('%d S1 products kept after comparison with DB jobs'%len(dico_s1_search))

        
        #get parameters of job processing and create job list
        jobs = []
        for key, value in dico_s1_search.items():
            
            #compute intersection with S2 tiles
            s1_shape = Polygon([(float(el.split(',')[0]), float(el.split(',')[1])) \
                for el in value.other_metadata.gmlgeometry.split('<gml:coordinates>')[-1].split('</gml:coordinates>')[0].split(' ')])
            s2tiles_intersect_list = []
            for tile_id, geometry_s2 in s2_tile_geometry_dict.items():
                if s1_shape.intersects(geometry_s2) and tile_id in RlieS1Job.__TILE_IDS:
                    s2tiles_intersect_list.append(tile_id)
            s2tiles_intersect_list = sorted(s2tiles_intersect_list)
            
            #build job
            job_loc = RlieS1Job(tile_id=key, \
                s1grd_id = key, \
                product_path = value.product_path, \
                measurement_date_start = value.sentinel1_id.start_time, \
                measurement_date_end = value.sentinel1_id.stop_time, \
                s1grd_dias_publication_date = value.dias_publication_date,\
                s2tile_ids_json=s2tiles_intersect_list,\
                reprocessing_context='nrt')
            jobs.append(job_loc)

        return jobs, []


    @staticmethod
    def get_jobs_without_esa_publication_date(logger_func):
        '''
        Retrieve all the RLIE-S1 jobs inserted in the database which have
        no esa publication date set.

        :param logger_func: logger.debug or logger.info or ...
        '''

        return RlieS1Job().attribute_is(
            's1grd_esa_publication_date', 'null').get(logger_func)


    @staticmethod
    def configure_batch_jobs(jobs, logger_func):
        '''
        Perform input product specific configuration, on a batch of jobs, which
        are required for their processing.

        :param jobs: list of jobs to be configured.
        :param logger_func: Logger instance
        '''

        if len(jobs) == 0:
            return jobs

        # Update the jobs with information found in the ESA hub.
        jobs = RlieS1Job.update_jobs_with_esa_info(jobs, logger_func)

        # Fix ESA publication date for already configured jobs which are
        # lacking this parameter.
        RlieS1Job.fix_esa_publication_date(logger_func)

        return jobs


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
                "The following S1 products are missing from the ESA request: \n - %s" % (
                    '\n - '.join(missing)))
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
        jobs = RlieS1Job.get_jobs_without_esa_publication_date(logger_func=logger.debug)

        # Keep only jobs that already have been processed
        jobs = [
            job
            for job in jobs
            if job.last_status_id is not None
            and job.last_status_id >= JobStatus.done.value
        ]

        # Exit if no jobs
        if not jobs:
            logger.info('No %s configured jobs require an ESA publication date update' %RlieS1Job.JOB_NAME)
            return

        logger.info('Update the ESA publication date of %d configured %s jobs' % (
            len(jobs), RlieS1Job.JOB_NAME))

        # Update the jobs with information found in the ESA hub, but do not change the job status.
        jobs = RlieS1Job.update_jobs_with_esa_info(jobs, logger, status_update=False)

        for job in jobs:
            job.patch(patch_foreign=True, logger_func=logger.debug)
