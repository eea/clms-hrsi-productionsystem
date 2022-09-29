#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from si_common.common_functions import *
from product_request_and_download.parse_cosims_products_wekeo import WekeoCosimsProductParser
import collections


def add_info_from_wekeo_cosims_products_matching_db(jobs, start_date=None, end_date=None):
    
    sensing_dates = sorted([job['measurement_date'] for job in jobs])
    if start_date is None:
        start_date = sensing_dates[0]
    if end_date is None:
        end_date = sensing_dates[-1]
        
    product_ids = set()
    for job in jobs:
        if job['fsc_path'] is not None:
            product_ids.add(os.path.basename(job['fsc_path']))
        if job['rlie_path'] is not None:
            product_ids.add(os.path.basename(job['rlie_path']))
    wekeo_product_dict = WekeoCosimsProductParser().search(start_date, end_date, properties_selection=['published'])
    missing_keys = product_ids - set(wekeo_product_dict.keys())
    if len(missing_keys) > 0:
        wekeo_product_dict.update(WekeoCosimsProductParser().get_product_info(list(missing_keys), properties_selection=['published'], verbose=0, error_mode=False))

    job_ids_remove = set()
    for i0, job in enumerate(jobs):
        if job['fsc_path'] is not None:
            if os.path.basename(job['fsc_path']) in wekeo_product_dict:
                job['fsc_dias_publication_date'] = wekeo_product_dict[os.path.basename(job['fsc_path'])]['published']
            else:
                # ~ print('  -> %s missing from DIAS'%os.path.basename(job['fsc_path']))
                # ~ job_ids_remove.add(i0)
                raise Exception('  -> %s missing from DIAS'%os.path.basename(job['fsc_path']))
        if job['rlie_path'] is not None:
            if os.path.basename(job['rlie_path']) in wekeo_product_dict:
                job['rlie_dias_publication_date'] = wekeo_product_dict[os.path.basename(job['rlie_path'])]['published']
            else:
                # ~ print('  -> %s missing from DIAS'%os.path.basename(job['rlie_path']))
                # ~ job_ids_remove.add(i0)
                raise Exception('  -> %s missing from DIAS'%os.path.basename(job['rlie_path']))
                
    print('%d jobs removed because they were not found on DIAS API'%(len(job_ids_remove)))
    jobs = [job for ii, job in enumerate(jobs) if ii not in job_ids_remove]

    return jobs
    
    
def json_http_request_hack(http_request):
    output = subprocess.check_output(['curl', http_request]).decode('utf-8')
    output = json.loads(output)
    return output
    

class CosimsProductParser:
    
    def post_process_jobs_metadata(self, jobs, remove_duplicates=True, verbose=0, add_wekeo_info=False, add_wekeo_info_search_dates=None):

        #date conversion to date time and identify jobs with indentical L1C ids
        l1c_id_dict = dict()
        for ii, job in enumerate(jobs):
            job['l1c_id'] = job['l1c_id'].replace('.SAFE','')
            job['l1c_id'] += '.SAFE'
            for key in job.keys():
                if 'date' in key:
                    job[key] = get_datetime_simple(job[key])
            job['processing_type'] = 'reprocessing'
            if 'nrt' in job:
                if job['nrt']:
                    job['processing_type'] = 'standard'
                
            if job['l1c_id'] in l1c_id_dict:
                l1c_id_dict[job['l1c_id']].add(ii)
            else:
                l1c_id_dict[job['l1c_id']] = set([ii])

        if remove_duplicates:
            #check that job with single L1C id are of processing type standard and jobs with 2 identical L1C ids are one standard and one reprocessed
            job_ids_delete = set()
            for l1c_id, ens in l1c_id_dict.items():
                job_ids_delete_loc = set()
                id_standard_keep, id_reprocessing_keep = None, None
                for ii in ens:
                    if jobs[ii]['processing_type'] == 'standard':
                        if id_standard_keep is None:
                            id_standard_keep = ii
                        else:
                            job_ids_delete_loc.add(ii)
                    elif jobs[ii]['processing_type'] == 'reprocessing':
                        if id_reprocessing_keep is None:
                            id_reprocessing_keep = ii
                        else:
                            job_ids_delete_loc.add(ii)
                    else:
                        raise Exception('processing_type %s unknown'%jobs[ii]['processing_type'])
                if len(job_ids_delete_loc) > 0:
                    if verbose > 0:
                        print('WARNING : got %d jobs for L1C %s, keeping only the most relevant...'%(len(ens), l1c_id))
                job_ids_delete |= job_ids_delete_loc
            jobs = [job for ii, job in enumerate(jobs) if ii not in job_ids_delete]
                
        if add_wekeo_info:
            if add_wekeo_info_search_dates is not None:
                jobs = add_info_from_wekeo_cosims_products_matching_db(jobs, start_date=add_wekeo_info_search_dates['start_date'], end_date=add_wekeo_info_search_dates['end_date'])
            else:
                jobs = add_info_from_wekeo_cosims_products_matching_db(jobs)
        return jobs
        
        
        
    def search_matching(self, matching_dict, remove_duplicates=True, add_wekeo_info=False):
        cmd = '/fsc_rlie_jobs?=and(%s)'%(','.join(['%s==%s'%(key, value) for key, value in matching_dict.items()]))
        jobs =  json_http_request_hack(os.environ['COSIMS_DB_HTTP_API_BASE_URL'] + cmd)
        return self.post_process_jobs_metadata(jobs, remove_duplicates=remove_duplicates, add_wekeo_info=add_wekeo_info)
        
        
    def search_date(self, start_date, end_date, date_field='measurement_date', remove_duplicates=True, add_wekeo_info=False):
        start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
        end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
        cmd = '/fsc_rlie_jobs?and=({0}.gte.{1},{0}.lt.{2})'.format(date_field, start_date_str, end_date_str)
        jobs =  json_http_request_hack(os.environ['COSIMS_DB_HTTP_API_BASE_URL'] + cmd)
        return self.post_process_jobs_metadata(jobs, remove_duplicates=remove_duplicates, add_wekeo_info=add_wekeo_info, \
            add_wekeo_info_search_dates={'start_date': start_date, 'end_date': end_date})
        

    def search_monthly_report(self, start_date, end_date, add_wekeo_info=False):
        
        cosims_product_parser = CosimsProductParser()
        jobs = cosims_product_parser.search_date(start_date, end_date, date_field='l1c_esa_publication_date', remove_duplicates=True)
        jobs = [job for job in jobs if job['processing_type'] == 'standard']
        for job in jobs:
            job['request_selection'] = 'l1c_tracking'
        job_first_selection = {job['l1c_id']: job for job in jobs}
        l1c_ids = set(job_first_selection.keys())
        
        jobs = cosims_product_parser.search_date(start_date, end_date, date_field='fsc_completion_date', remove_duplicates=True)
        new_l1c_ids = set()
        for ii in range(len(jobs)):
            new_l1c_ids.add(jobs[ii]['l1c_id'])
            if jobs[ii]['processing_type'] == 'standard' and jobs[ii]['l1c_id'] in l1c_ids:
                jobs[ii]['request_selection'] = 'both'
            else:
                jobs[ii]['request_selection'] = 'generated_within_period'
        
        for l1c_id in l1c_ids - new_l1c_ids:
            jobs.append(job_first_selection[l1c_id])
            
        return self.post_process_jobs_metadata(jobs, remove_duplicates=False, add_wekeo_info=add_wekeo_info)

        
        
def interpret_values_as_json_and_dates(job):
    
    for key in job.keys():
        if not isinstance(job[key], str):
            continue
        if len(job[key]) == 0:
            continue
        if job[key][0] in ['{', '[']:
            job[key] = load_json(job[key])
        elif key in ['fsc_publication_date_list', 'fsc_measurement_date_list', 'wds_publication_date_list', 'wds_measurement_date_list', \
            'sws_publication_date_list', 'sws_measurement_date_list', 'obsolete_product_id_list']:
            job[key] = job[key].split(';')
        if 'date' in key or 'time' in key:
            if isinstance(job[key], str):
                try:
                    job[key] = str2datetime_generic(job[key])
                except:
                    raise Exception('conversion to date failed for %s: %s'%(key, str(job[key])))
                    set_trace()
            elif isinstance(job[key], dict):
                job[key] = {el1: str2datetime_generic(job[key][el2]) for el1, el2 in job[key].items()}
            elif isinstance(job[key], list):
                job[key] = [str2datetime_generic(el) for el in job[key]]
            else:
                raise Exception('unhandled type in interpret_values_as_json_and_dates: %s'%type(job[key]))
    return job
        
    

class CosimsProductParserGeneric:
    
    table_list = ['fsc_rlie_jobs', 'rlies1_jobs', 'sws_wds_jobs', 'rlies1s2_jobs', 'gfsc_jobs']
    parent_table = 'parent_jobs'
    status_to_id = {'initialized': 1,
        'configured': 2,
        'ready': 3,
        'queued': 4,
        'started': 5,
        'pre_processing': 6,
        'processing': 7,
        'post_processing': 8,
        'processed': 9,
        'start_publication': 10,
        'published': 11,
        'done': 12,
        'internal_error': 13,
        'external_error': 14,
        'error_checked': 15,
        'cancelled': 16}
    status_from_id = {value: key for key, value in status_to_id.items()}
        
    @staticmethod
    def generic_add_wekeo_info_to_jobs(jobs):
        """add publication date and productIdentifier information to jobs : must be gathered from wekeo API"""
        
        product_list = []
        for job in jobs:
            for el in ['fsc_path', 'rlie_path', 'sws_path', 'wds_path', 'gfsc_path', 'rlies1s2_path', 'rlies1_product_paths_json']:
                if el not in job:
                    continue
                if job[el] is None:
                    continue
                if el == 'rlies1_product_paths_json':
                    if job['job_table'] != 'rlies1_jobs':
                        continue
                    assert isinstance(job[el], dict)
                    for elem in job[el].values():
                        product_list.append(os.path.basename(elem))
                else:
                    assert isinstance(job[el], str)
                    product_list.append(os.path.basename(job[el]))
        product_list_unique = sorted(list(set(product_list)))
        if len(product_list) != len(product_list_unique):
            print('duplicate products in COSIMS jobs:\n%s'%('\n'.join([item for item, count in collections.Counter(product_list).items() if count > 1])))
                
        print('Getting Wekeo information for %d HR-SI products...'%len(product_list_unique))
        time_start = datetime.utcnow()
        wekeo_info = WekeoCosimsProductParser().get_product_info_smartsearch(product_list_unique, properties_selection=['published', 'productIdentifier'], \
            verbose=0, error_mode=True, nprocs=None)
        print('  -> done in %s'%(datetime.utcnow() - time_start))
            
        for ii, job in enumerate(jobs):
            jobs[ii]['wekeo_publication_dates'] = dict()
            jobs[ii]['wekeo_product_identifiers'] = dict()
            for el in ['fsc_path', 'rlie_path', 'sws_path', 'wds_path', 'gfsc_path', 'rlies1s2_path', 'rlies1_product_paths_json']:
                if el not in job:
                    continue
                if job[el] is None:
                    continue
                if el == 'rlies1_product_paths_json':
                    if job['job_table'] != 'rlies1_jobs':
                        continue
                    for key, value in job[el].items():
                        if key not in wekeo_info:
                            raise Exception('product %s missing from wekeo_info'%key)
                            set_trace()
                        jobs[ii]['wekeo_publication_dates'][key] = wekeo_info[key]['published']
                        jobs[ii]['wekeo_product_identifiers'][key] = wekeo_info[key]['productIdentifier']
                else:
                    key = os.path.basename(job[el])
                    if key not in wekeo_info:
                        raise Exception('product %s missing from wekeo_info'%key)
                        set_trace()
                    jobs[ii]['wekeo_publication_dates'][key] = wekeo_info[key]['published']
                    jobs[ii]['wekeo_product_identifiers'][key] = wekeo_info[key]['productIdentifier']
            
        return jobs
        
        
    def search_date(self, job_table, start_date, end_date, date_field=None, add_wekeo_info=False):
        """Search a job child table in a date interval
        Option : fill the job table with a wekeo_publication_dates dict containing wekeo publication dates recovered from Wekeo API
        """
        
        start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
        end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
        if date_field is None:
            if job_table == 'rlies1_jobs':
                date_field = 'measurement_date_start'
            elif job_table == 'rlies1s2_jobs':
                date_field = 'process_date'
            elif job_table == 'gfsc_jobs':
                date_field = 'sensing_start_date'
            else:
                date_field = 'measurement_date'
        cmd = '/{0}?and=({1}.gte.{2},{1}.lt.{3})'.format(job_table, date_field, start_date_str, end_date_str)
        print(os.environ['COSIMS_DB_HTTP_API_BASE_URL'] + cmd)
        jobs =  json_http_request_hack(os.environ['COSIMS_DB_HTTP_API_BASE_URL'] + cmd)
        for ii in range(len(jobs)):
            jobs[ii] = interpret_values_as_json_and_dates(jobs[ii])
        if add_wekeo_info:
            jobs = self.generic_add_wekeo_info_to_jobs(jobs)
        return jobs
        
        
    def get_all_products_info(self, start_date, end_date, add_wekeo_info=False):
        """Get all child tables in a date interval, recover their status from parent table
        Option : fill the job table with a wekeo_publication_dates dict containing wekeo publication dates recovered from Wekeo API
        """
        
        jobs = []
        for job_table in self.table_list:
            print('Getting job table: %s'%job_table)
            jobs_loc = self.search_date(job_table, start_date, end_date, add_wekeo_info=False)
            for job in jobs_loc:
                job['job_table'] = job_table
            jobs += jobs_loc
        
        print('Getting parent job table...')
        parent_jobs = json_http_request_hack(os.environ['COSIMS_DB_HTTP_API_BASE_URL'] + '/' + self.parent_table + '?select=id,last_status_id,tile_id')
        parent_jobs = {job['id']: job for job in parent_jobs}
        for job in jobs:
            if job['job_table'] in ['sws_wds_jobs', 'gfsc_jobs']:
                job['tile_id'] = parent_jobs[job['fk_parent_job_id']]['tile_id']
            job['status_parent_id'] = parent_jobs[job['fk_parent_job_id']]['last_status_id']
            if job['status_parent_id'] is None:
                job['status_parent_id'] = 1
            job['status_parent_id'] = int(job['status_parent_id'])
            job['status_parent'] = self.status_from_id[job['status_parent_id']]
            if job['status_parent_id'] in [11, 12]:
                job['simple_status_parent'] = 'successful'
            elif job['status_parent_id'] < 11:
                job['simple_status_parent'] = 'in_progress'
            elif job['status_parent_id'] == 14:
                job['simple_status_parent'] = 'system_error'
            elif job['status_parent_id'] in [15, 16]:
                job['simple_status_parent'] = 'definitive_error'
            else:
                job['simple_status_parent'] = 'error'
        del parent_jobs
        
        if add_wekeo_info:
            print('Adding generic wekeo info to jobs...')
            jobs = self.generic_add_wekeo_info_to_jobs(jobs)
        return jobs
    
    

    
    
