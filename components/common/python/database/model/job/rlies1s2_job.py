import os, sys, shutil
import numpy as np
import json
from datetime import datetime, timedelta
from shapely.geometry import Polygon

from .job_template import JobTemplate
from .job_status import JobStatus
from .worker_flavors import WorkerFlavors
from ....util.datetime_util import DatetimeUtil
from ....util.eea39_util import Eea39Util
from ....util.creodias_util import CreodiasUtil
from ...rest.stored_procedure import StoredProcedure
from .system_parameters import SystemPrameters
from .fsc_rlie_job import FscRlieJob
from .rlies1_job import RlieS1Job


class RlieS1S2Job(JobTemplate):
    '''
    Description of a generic test job, used for integration validation purpose.
    '''
    
    ################################################################
    # Start of specific properties
    
    # Database table name
    __TABLE_NAME = "rlies1s2_jobs"

    # Class job name
    JOB_NAME = "rlies1s2"

    # Sentinel input product type
    INPUT_PRODUCT_TYPE = "rlie"

    # Name of the Nomad job processing the present job type
    NOMAD_JOB_NAME = "rlies1s2-processing"

    # Worker flavor required for the present job type processing
    WORKER_FLAVOR_NAME = WorkerFlavors.medium.value

    # Name of the products the job will generate during it's processing
    OUTPUT_PRODUCTS_LIST = ["rlies1s2"]
    
    # Restrict the orchestrator to a specific tile ID.
    # __TILE_RESTRICTION = ['30TYN', '32TLR', '32TMS', '33WWR', '33WXS', '34WBD', 
    #                       '32TLS', '32TLT', '38SLJ', '38SMJ', '38TMK']
    __TILE_RESTRICTION = None

    # List of all the EEA39 Sentinel-2 tile IDs or filtered accordint __TILE_RESTRICTION.
    __TILE_IDS = Eea39Util.get_tiles(tile_restriction=__TILE_RESTRICTION)
    __TILE_IDS = Eea39Util.select_rlie_zone_tile_ids(__TILE_IDS)

    # Keep track of the last FSC RLIE job inserted in the database.
    LAST_INSERTED_JOB = None

    # Name of the stored procedure used to retrieve TestJob with a given status
    GET_JOBS_WITH_STATUS_PROCEDURE_NAME = "rlies1s2_jobs_with_last_status"

    ################################################################
    # End of specific properties


    def __init__(self, **kwds):
        
        #filled in job creation
        self.process_date = None
        self.tile_id_dup = None
        self.rlies1_product_paths_json = None
        self.rlies1_publication_latest_date = None
        self.rlies2_product_paths_json = None
        self.rlies2_publication_latest_date = None
        self.reprocessing_context = None
        
        #filled in job configuration
        
        #filled in worker after job processing
        self.measurement_date_rlies1s2 = None
        self.rlies1s2_completion_date = None
        self.rlies1s2_path = None
        self.rlies1s2_json_submitted_json = None
        
        #filled in post-processing / publication stage (get_products_publication_jsons)
        self.publication_date_rlies1s2 = None

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
    def get_job_with_last_rlie_publication_date(self, rlie_type):
        if rlie_type == 'rlies1':
            attribute_name = 'publication_date_rlies1'
        elif rlie_type == 'rlies2':
            attribute_name = 'publication_date_rlies2'
        elif rlie_type == 'rlies1s2':
            attribute_name = 'publication_date_rlies1s2'
        else:
            raise Exception('unknown rlie_type %s'%rlie_type)
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
        
        return [self.rlies1s2_json_submitted_json]


    def generated_a_product(self):
        '''Return a boolean to notify if the job did generate a product or not.'''

        if self.rlies1s2_json_submitted_json is not None:
            return True
        return False


    def set_product_publication_date(self, product_name: str, publication_json: dict = None):
        '''
        Set the publication date of a given type of product.

        :param product_name: name of the product to update the publication date.
        '''

        self.publication_date_rlies1s2 = datetime.utcnow()
        if publication_json is not None:
            self.rlies1s2_json_submitted_json = publication_json
        


    ################################################################
    # Start of static specifics methods
    
    ################
    #only for test purposes
    @staticmethod
    def make_fake_matching_rlie_jobs(logger):
        
        logger.info('FS1S2: start')
        
        #select search window
        search_start = SystemPrameters().get(logger.debug).rlies1s2_earliest_date
        assert search_start is not None
        if not isinstance(search_start, datetime):
            print('converting to datetime')
            search_start = DatetimeUtil.fromRfc3339(search_start)
        search_end = datetime.utcnow()
        
        # get s1 and s2 jobs
        s1jobs = StoredProcedure.get_jobs_within_measurement_date(RlieS1Job(), 'measurement_date_start', search_start, search_end, logger.info)
        s2jobs = StoredProcedure.get_jobs_within_measurement_date(FscRlieJob(), 'measurement_date', search_start, search_end, logger.info)
        
        # build list of matching rlie s1 and s2 jobs
        #1) get matching dates
        days = sorted([datetime.strptime(day_loc, '%Y%m%d') for day_loc in set([job.measurement_date_start.strftime('%Y%m%d') for job in s1jobs]).intersection(set([job.measurement_date.strftime('%Y%m%d') \
            for job in s2jobs]))])
        
        #2) select 1 S1 and 1 S2 per matching date
        product_list_gen = []
        for day in days:
            rlies1job_loc = [job for job in s1jobs if job.measurement_date_start >= day and job.measurement_date_start < day+timedelta(1)][0]
            rlies2job_loc = [job for job in s2jobs if job.measurement_date >= day and job.measurement_date < day+timedelta(1)][0]
            tile_id = rlies2job_loc.tile_id
            logger.info('FS1S2: day %s, S2 tile %s'%(day.strftime('%Y-%m-%d'), tile_id))
            #get dem
            product_name_s1 = 'RLIE_%s_%s_T%s_%s_%s'%(rlies1job_loc.measurement_date_start.strftime('%Y%m%dT%H%M%S'), rlies1job_loc.s1grd_id.split('_')[0], tile_id, 'V000', '1')
            product_name_s2 = 'RLIE_%s_%s_T%s_%s_%s'%(rlies2job_loc.measurement_date.strftime('%Y%m%dT%H%M%S'), rlies2job_loc.l1c_id.split('_')[0], tile_id, 'V000', '1')
            product_path_s1 = 'CLMS/Pan-European/High_Resolution_Layers/Ice/RLIE_S1/%s/%s'%(rlies1job_loc.measurement_date_start.strftime('%Y/%m/%d'), product_name_s1)
            product_path_s2 = 'CLMS/Pan-European/High_Resolution_Layers/Ice/RLIE/%s/%s'%(rlies2job_loc.measurement_date.strftime('%Y/%m/%d'), product_name_s2)
            product_list_gen += [product_path_s1, product_path_s2]


            
            #3) edit S1 and S2 job attributes
            #S1 job

            #filled in worker after job processing
            rlies1job_loc.rlies1_products_completion_date = datetime.utcnow()
            rlies1job_loc.rlies1_product_paths_json = {product_name_s1: product_path_s1}
            rlies1job_loc.rlies1_product_json_submitted_json = {product_name_s1: dict()}
        
            #filled in post-processing / publication stage (get_products_publication_jsons)
            rlies1job_loc.rlies1_products_publication_date = datetime.utcnow()
            
            #status change
            rlies1job_loc.post_new_status_change(JobStatus.configured)
            rlies1job_loc.post_new_status_change(JobStatus.ready)
            rlies1job_loc.post_new_status_change(JobStatus.queued)
            rlies1job_loc.post_new_status_change(JobStatus.started)
            rlies1job_loc.post_new_status_change(JobStatus.pre_processing)
            rlies1job_loc.post_new_status_change(JobStatus.processing)
            rlies1job_loc.post_new_status_change(JobStatus.post_processing)
            rlies1job_loc.post_new_status_change(JobStatus.processed)
            rlies1job_loc.post_new_status_change(JobStatus.start_publication)
            rlies1job_loc.post_new_status_change(JobStatus.published)
            rlies1job_loc.post_new_status_change(JobStatus.done)
            rlies1job_loc.patch(patch_foreign=True, logger_func=logger.debug)
            logger.info('FS1S2:  -> updated S1 job')
            
            #S2 job
            
            # ~ rlies2job_loc.nrt = None
            # ~ rlies2job_loc.l1c_id_list = None
            # ~ rlies2job_loc.l1c_reference_job = None
            # ~ rlies2job_loc.l1c_path_list = None
            # ~ rlies2job_loc.l2a_path_in = None
            # ~ rlies2job_loc.l2a_path_out = None
            # ~ rlies2job_loc.save_full_l2a = None
            # ~ rlies2job_loc.job_id_for_last_valid_l2a = None
            # ~ rlies2job_loc.l2a_status = None
            # ~ rlies2job_loc.n_jobs_run_since_last_init = None
            # ~ rlies2job_loc.n_l2a_produced_since_last_init = None
            # ~ rlies2job_loc.dtm_path = None
            # ~ rlies2job_loc.fsc_infos = None
            # ~ rlies2job_loc.rlie_infos = None
            # ~ rlies2job_loc.fsc_path = None
            rlies2job_loc.rlie_path = product_path_s2
            # ~ rlies2job_loc.l1c_sensing_time = None
            # ~ rlies2job_loc.l1c_esa_publication_date = None
            # ~ rlies2job_loc.fsc_completion_date = None
            rlies2job_loc.rlie_completion_date = datetime.utcnow()
            # ~ rlies2job_loc.fsc_json_publication_date = None
            rlies2job_loc.rlie_json_publication_date = datetime.utcnow()
            # ~ rlies2job_loc.maja_mode = None
            # ~ rlies2job_loc.maja_threads = None
            # ~ rlies2job_loc.maja_other_params = None
            # ~ rlies2job_loc.maja_return_code = None
            # ~ rlies2job_loc.backward_reprocessing_run = None
            # ~ rlies2job_loc.reprocessing_context = None
            
            rlies2job_loc.post_new_status_change(JobStatus.configured)
            rlies2job_loc.post_new_status_change(JobStatus.ready)
            rlies2job_loc.post_new_status_change(JobStatus.queued)
            rlies2job_loc.post_new_status_change(JobStatus.started)
            rlies2job_loc.post_new_status_change(JobStatus.pre_processing)
            rlies2job_loc.post_new_status_change(JobStatus.processing)
            rlies2job_loc.post_new_status_change(JobStatus.post_processing)
            rlies2job_loc.post_new_status_change(JobStatus.processed)
            rlies2job_loc.post_new_status_change(JobStatus.start_publication)
            rlies2job_loc.post_new_status_change(JobStatus.published)
            rlies2job_loc.post_new_status_change(JobStatus.done)
            rlies2job_loc.patch(patch_foreign=True, logger_func=logger.debug)
            logger.info('FS1S2:  -> updated S2 job')
            
        print('To run RLIES1+S2 worker, you must generate the following products:\n%s\n'%('\n'.join(product_list_gen)))
        
    ################
    
    
    

    @staticmethod
    def get_jobs_to_create(internal_database_parallel_request: int, logger):
        '''
        Request an API for new input products and determine the list of jobs 
        which should be created from it. This list of job can be filtered and ordered.
        
        :param internal_database_parallel_request: number of requests which can 
            be performed in parallel to the database.
        :param logger: Logger instance.
        '''
        
        date_now = datetime.utcnow()
        accepted_status = set(range(1, 17)) - {13, 16}
        sys_params = SystemPrameters().get(logger.debug)
        if sys_params.rlies1s2_sleep_seconds_between_loop is not None:
            logger.info('Pausing for %d seconds...'%sys_params.rlies1s2_sleep_seconds_between_loop)
            time.sleep(sys_params.rlies1s2_sleep_seconds_between_loop)
            
        assert sys_params.rlies1s2_earliest_date is not None
        if not isinstance(sys_params.rlies1s2_earliest_date, datetime):
            sys_params.rlies1s2_earliest_date = DatetimeUtil.fromRfc3339(sys_params.rlies1s2_earliest_date)
        
        # ~ #########################
        # ~ job_s1s2_list = StoredProcedure.get_jobs_within_measurement_date(RlieS1S2Job(), 'process_date', sys_params.rlies1s2_earliest_date, datetime.utcnow(), logger.info)
        # ~ logger.info('rlie s1+s2: %d'%len(job_s1s2_list))
        # ~ if len(job_s1s2_list) == 0:
            # ~ #REMOVE LATER !!!! => makes fake rlie s1 and fsc rlie jobs to test RLIES1S2 job creation
            # ~ RlieS1S2Job.make_fake_matching_rlie_jobs(logger)
        # ~ else:
            # ~ input('y/n')
        # ~ #########################
        
        
        
        
        #start with previous day from now and go backwards
        start_day = date_now-timedelta(1)
        start_day = datetime(start_day.year, start_day.month, start_day.day) #set date to beginning of day at 00:00:00
        
        #if start_day + timedelta(1) (end of day) is less than rlies1s2_min_delay_from_end_of_day_hours from now, then start one day before
        if sys_params.rlies1s2_min_delay_from_end_of_day_hours is not None:
            if date_now - start_day + timedelta(1) < timedelta(hours=sys_params.rlies1s2_min_delay_from_end_of_day_hours):
                start_day -= timedelta(1)

        initial_start_day = start_day

        #check if fscrlie jobs or rlies1 jobs exist within the start_day, if not then roll start_day backwards until 1 fscrlie and 1 rlies1 job is found
        #-> this is done to handle system restarts after a long period of time
        while(True):
            candidates_s1 = StoredProcedure.get_jobs_within_measurement_date(RlieS1Job(), 'measurement_date_start', start_day, start_day + timedelta(1), logger.info)
            candidates_s2 = StoredProcedure.get_jobs_within_measurement_date(FscRlieJob(), 'measurement_date', start_day, start_day + timedelta(1), logger.info)
            if len(candidates_s1) > 0 and len(candidates_s2) > 0:
                break
            start_day -= timedelta(1)
            if start_day < sys_params.rlies1s2_earliest_date:
                break
            if sys_params.rlies1s2_max_search_window_days_absolute is not None:
                if abs(initial_start_day - start_day) > timedelta(sys_params.rlies1s2_max_search_window_days_absolute):
                    break
                    
        #iterate backwards on days until ndays_searched >= rlies1s2_max_search_window_days is reached or day_loc < rlies1s2_earliest_date or 
        #(ndays_searched >= rlies1s2_min_search_window_days and all possible RLIE S1+S2 jobs were created for day_loc)
        day_loc = start_day + timedelta(1)
        jobs = []
        jobs_day = []
        while(True):
            
            day_loc -= timedelta(1)
            logger.info('Looking for RLIE S1 and S2 intersection on day %s'%day_loc.strftime('%Y-%m-%d'))
            
            if day_loc < sys_params.rlies1s2_earliest_date:
                logger.info('  -> Stopping search because day_loc < rlies1s2_earliest_date : %s...'%sys_params.rlies1s2_earliest_date.strftime('%Y-%m-%d'))
                break
            if sys_params.rlies1s2_max_search_window_days is not None:
                if abs(day_loc - start_day) > timedelta(sys_params.rlies1s2_max_search_window_days):
                    logger.info('  -> Stopping search because day_loc - start_day (%s) > rlies1s2_max_search_window_days (%s)...'%(start_day.strftime('%Y-%m-%d'), \
                        sys_params.rlies1s2_max_search_window_days))
                    break
            if len(jobs_day) == 0 and abs(day_loc - start_day) > timedelta(sys_params.rlies1s2_min_search_window_days):
                logger.info('  -> Stopping search because no job left to be created for day_loc and day_loc - start_day (%s) > rlies1s2_min_search_window_days (%s)...'%(start_day.strftime('%Y-%m-%d'), \
                    sys_params.rlies1s2_min_search_window_days))
                break
            jobs_day = []
            
            logger.info('  -> Getting RLIE S1 jobs with accepted status for day_loc')
            candidates_s1 = [el for el in StoredProcedure.get_jobs_within_measurement_date(RlieS1Job(), 'measurement_date_start', day_loc, day_loc + timedelta(1), logger.info) \
                if el.last_status_id in accepted_status]
            logger.info('  -> Getting RLIE S2 jobs with accepted status for day_loc')
            candidates_s2 = [el for el in StoredProcedure.get_jobs_within_measurement_date(FscRlieJob(), 'measurement_date', day_loc, day_loc + timedelta(1), logger.info) \
                if el.last_status_id in accepted_status]
            logger.info('  -> Getting tile ids for which RLIE S1+S2 job was already created (for day_loc)')
            tile_ids_existing_s1s2_job = set([el.tile_id for el in StoredProcedure.get_jobs_within_measurement_date(RlieS1S2Job(), 'process_date', day_loc-timedelta(hours=1), \
                day_loc + timedelta(hours=1), logger.info)])
                
            #for S1 list of tile ids are only computed in worker instance so we don't have the information of which tiles will be produced until job is finished, 
            #therefore all RLIE S1 jobs within the day must be finished for RLIE S1+S2 production, 
            #unless now - day_loc + timedelta(1) > timedelta(hours=rlies1s2_max_delay_from_end_of_day_hours_wait_for_rlie_products)
            candidates_s1_loc = [el for el in candidates_s1 if (el.rlies1_product_paths_json is not None and el.rlies1_products_publication_date is not None)] #select jobs that have published products
            if (date_now - day_loc + timedelta(1) <= timedelta(hours=sys_params.rlies1s2_max_delay_from_end_of_day_hours_wait_for_rlie_products)) and (len(candidates_s1_loc) < len(candidates_s1)):
                logger.info(' -> End of day %s is less than %d hours ago and %d RLIE S1 jobs are still pending processing'%(day_loc.strftime('%Y-%m-%d'), \
                    sys_params.rlies1s2_max_delay_from_end_of_day_hours_wait_for_rlie_products, len(candidates_s1)-len(candidates_s1_loc)) + \
                    ', so additional intersections may be possible, skipping...')
                continue
                
            for tile_id in sorted(list(set(RlieS1S2Job.__TILE_IDS) - tile_ids_existing_s1s2_job)):

                #select S2
                jobs_rlie_s2_match = [el for el in candidates_s2 if (el.rlie_json_publication_date is not None) and \
                    (el.rlie_path is not None) and (os.path.basename(el.rlie_path).split('_')[-3][1:] == tile_id)] #select jobs that have published products
                product_paths_rlie_s2_match = [el.rlie_path for el in jobs_rlie_s2_match]
                publication_date_rlie_s2_match = [el.rlie_json_publication_date for el in jobs_rlie_s2_match]
                if len(product_paths_rlie_s2_match) == 0:
                    continue

                rlies2_publication_latest_date = max(publication_date_rlie_s2_match)
                    
                av_s2_measurement_date = datetime(2000,1,1) + timedelta(seconds = np.mean([(datetime.strptime(os.path.basename(el).split('_')[-5], '%Y%m%dT%H%M%S') - \
                    datetime(2000,1,1)).total_seconds() for el in product_paths_rlie_s2_match]))
                
                #select S1
                product_paths_rlie_s1_match_per_sat = dict()
                publication_date_rlie_s1_match_per_sat = dict()
                for job_loc in candidates_s1_loc:
                    for path_loc in job_loc.rlies1_product_paths_json.values():
                        if os.path.basename(path_loc).split('_')[-3][1:] == tile_id:
                            sat_loc = os.path.basename(path_loc).split('_')[-4]
                            if sat_loc in product_paths_rlie_s1_match_per_sat:
                                product_paths_rlie_s1_match_per_sat[sat_loc].append(path_loc)
                                publication_date_rlie_s1_match_per_sat[sat_loc].append(job_loc.rlies1_products_publication_date)
                            else:
                                product_paths_rlie_s1_match_per_sat[sat_loc] = [path_loc]
                                publication_date_rlie_s1_match_per_sat[sat_loc] = [job_loc.rlies1_products_publication_date]
                if len(product_paths_rlie_s1_match_per_sat) == 0:
                    continue
                closest_date_sat = None
                dt_closest = None
                for sat, paths_loc in product_paths_rlie_s1_match_per_sat.items():
                    av_s1_measurement_date = datetime(2000,1,1) + timedelta(seconds = np.mean([(datetime.strptime(os.path.basename(el).split('_')[-5], '%Y%m%dT%H%M%S') - \
                        datetime(2000,1,1)).total_seconds() for el in paths_loc]))
                    dt_loc = abs(av_s1_measurement_date - av_s2_measurement_date)
                    if dt_closest is None:
                        closest_date_sat = sat
                        dt_closest = dt_loc
                    elif dt_loc < dt_closest:
                        closest_date_sat = sat
                        dt_closest = dt_loc
                
                logger.info('    S2 products match: %s'%(','.join(product_paths_rlie_s2_match)))
                logger.info('    S1 products match: %s'%(','.join(product_paths_rlie_s1_match_per_sat[closest_date_sat])))
                jobs_day.append(RlieS1S2Job(tile_id=tile_id,
                    process_date = day_loc,
                    tile_id_dup = tile_id,
                    rlies1_product_paths_json = product_paths_rlie_s1_match_per_sat[closest_date_sat],
                    rlies1_publication_latest_date = max(publication_date_rlie_s1_match_per_sat[closest_date_sat]),
                    rlies2_product_paths_json = product_paths_rlie_s2_match,
                    rlies2_publication_latest_date = rlies2_publication_latest_date,
                    reprocessing_context = 'nrt'))
                    
            jobs += jobs_day
                   
                    
        #reverse jobs order to submit oldest first (should yield better product turnaround although this product is not NRT)
        jobs = jobs[::-1]

                            
        logger.info('rlies1s2 job creation creating %d jobs'%len(jobs))

        return jobs, []


    @staticmethod
    def configure_batch_jobs(jobs, logger_func):
        '''
        Perform input product specific configuration, on a batch of jobs, which
        are required for their processing.

        :param jobs: list of jobs to be configured.
        :param logger_func: Logger instance
        '''

        return jobs
