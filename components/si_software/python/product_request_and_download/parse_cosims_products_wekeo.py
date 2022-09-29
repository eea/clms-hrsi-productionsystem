#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""chain processing on DIAS"""

from si_common.common_functions import *
import multiprocessing
from si_utils.rclone import Rclone
from si_utils.amalthee_tools import AmaltheeManager


    

def features_to_dict(features, properties_selection=None, tile_id_post_selection=None):
    if tile_id_post_selection is not None:
        if properties_selection is not None:
            return {feature['properties']['title']: convert_dates_in_wekeo_dict({key: feature['properties'][key] for key in properties_selection}) \
                for feature in features if feature['properties']['title'].split('_')[-3][1:] in tile_id_post_selection}
        else:
            return {feature['properties']['title']: convert_dates_in_wekeo_dict(feature['properties']) \
                for feature in features if feature['properties']['title'].split('_')[-3][1:] in tile_id_post_selection}
    else:
        if properties_selection is not None:
            return {feature['properties']['title']: convert_dates_in_wekeo_dict({key: feature['properties'][key] for key in properties_selection}) for feature in features}
        else:
            return {feature['properties']['title']: convert_dates_in_wekeo_dict(feature['properties']) for feature in features}
    
    


class WekeoCosimsProductParser:

    __main_url = 'https://cryo.land.copernicus.eu/resto/api/collections/HRSI/search.json'
    
    def get_product_info_smartsearch(self, product_list, properties_selection=None, verbose=0, error_mode=True, nprocs=None):
        

        n_products_min_use_search_periods = 10
        n_days_discontinuity_max_common_search_period = 5
        
        #case where it is a product str instead of a list of product str
        if isinstance(product_list, str):
            return self.get_product_info(product_list, properties_selection=properties_selection, verbose=verbose, error_mode=error_mode, nprocs=nprocs)
        
        #empty trivial case
        if len(product_list) == 0:
            return dict()
        
        product_names = [os.path.basename(el) for el in product_list]
        #if low number of products => request them directly product by product
        if len(product_names) < n_products_min_use_search_periods:
            return self.get_product_info(product_names, properties_selection=properties_selection, verbose=verbose, error_mode=error_mode, nprocs=nprocs)
        
        
        
        #for larger number of products, request products over search periods and then keep only the sought after products (lower number of requests i.e. much faster)
        
        #establish search periods in days with 
        dates_search_days = sorted(list(set([datetime.strptime(el.split('_')[1].split('T')[0].split('-')[0], '%Y%m%d') for el in product_names])))
        search_intervals = []
        date_min, date_max = None, None
        for ii in range(len(dates_search_days)):
            #init
            if date_min is None:
                date_min = dates_search_days[ii]
            if date_max is None:
                date_max = dates_search_days[ii]
            #loop
            elif abs(date_max - dates_search_days[ii]) <= timedelta(n_days_discontinuity_max_common_search_period):
                date_max = dates_search_days[ii]
            else:
                search_intervals.append([date_min, date_max])
                date_min, date_max = dates_search_days[ii], dates_search_days[ii]
        search_intervals.append([date_min, date_max])
            
        #request products on search intervals and keep only the sought after products
        product_names_set = set(product_names)
        dico = dict()
        for date_min, date_max in search_intervals:
            dico_loc = self.search(date_min, date_max, properties_selection=properties_selection, verbose=verbose)
            dico.update({key: value for key, value in dico_loc.items() if key in product_names_set})
        
        #in case of search bug, just check that all products were indeed recovered and if not try to get them individually
        missing_products = sorted(list(product_names_set - dico.keys()))
        if len(missing_products) > 0:
            print('Search did not recover all products in WekeoCosimsProductParser (%d/%d, %d missing), trying to get missing products individually...'%(len(product_names_set) - len(missing_products), \
                len(product_names_set), len(missing_products)))
            dico.update(self.get_product_info(missing_products, properties_selection=properties_selection, verbose=verbose, error_mode=error_mode, nprocs=nprocs))
        return dico
    
    
    def get_product_info(self, product_list, properties_selection=None, verbose=0, error_mode=True, nprocs=None):
        
        
        monooutput = False
        if isinstance(product_list, str):
            product_list = [product_list]
            monooutput = True
        assert isinstance(product_list, list)
        if len(product_list) == 0:
            return dict()
        time0 = time.time()
        
        if nprocs is None:
            nprocs = 20
        nprocs = min(nprocs, len(product_list))
        
        dico = dict()
        if nprocs <= 1:
            for product_id in product_list:
                dico_request = json_http_request_no_error(self.__main_url + '?' + 'productIdentifier=' + '%25' + product_id + '%25')
                if dico_request is not None:
                    dico.update(features_to_dict(dico_request['features'], properties_selection=properties_selection))
        else:
            pool = multiprocessing.Pool(min(nprocs, len(product_list)))
            url_requests = [self.__main_url + '?' + 'productIdentifier=' + '%25' + product_id + '%25' for product_id in product_list]
            for ii, dico_request in enumerate(pool.map(json_http_request_no_error, url_requests)):
                if dico_request is None:
                    print('request failed: %s, retrying...'%url_requests[ii])
                    dico_request = json_http_request_no_error(url_requests[ii])
                if dico_request is None:
                    print('request failed: %s'%url_requests[ii])
                    continue
                dico.update(features_to_dict(dico_request['features'], properties_selection=properties_selection))
        
        if set(product_list) != set(list(dico.keys())):
            print('The following products were not retrieved :\n%s\n'%('\n'.join(['- %s'%el for el in sorted(list(set(product_list) - set(list(dico.keys()))))])))
            print('The following products were retrieved but do not match requested product ids:\n%s\n'%('\n'.join(['- %s'%el for el in sorted(list(set(list(dico.keys())) - set(product_list)))])))
            if error_mode:
                raise Exception('product request and retrieval mismatch')
        
        if verbose > 0:
            print('  -> Executed in %s seconds'%(time.time()-time0))
            
        if monooutput:
            return dico[product_list[0]]
        return dico
        
    
    def search(self, date_min, date_max, tile_id=None, footprint_wkt=None, tile_id_post_selection=None, properties_selection=None, output_file=None, verbose=0):
        
        assert date_max > date_min
            
        url_params = dict()
        url_params['maxRecords'] = '1000'
        url_params['status'] = 'all'
        url_params['dataset'] = 'ESA-DATASET'
        if tile_id is not None:
            url_params['productIdentifier'] = '%25' + tile_id + '%25'
            url_params['maxRecords'] = max(10, (date_max-date_min).days*2)
        if footprint_wkt is not None:
            url_params['geometry'] = '{}'.format(footprint_wkt.replace('POLYGON ','POLYGON').replace(', ',',').replace(' ','+'))
        url_params['startDate'] = date_min.strftime('%Y-%m-%dT%H:%M:%SZ')
        url_params['completionDate'] = date_max.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        full_url = self.__main_url + '?' + '&'.join(['%s=%s'%(key, value) for key, value in url_params.items()])
        dico_request = json_http_request_no_error(full_url)
        dico = features_to_dict(dico_request['features'], tile_id_post_selection=tile_id_post_selection, properties_selection=properties_selection)
        
        id_last = [ii for ii in range(len(dico_request['properties']['links'])) if dico_request['properties']['links'][ii]['rel'] == 'last']
        assert len(id_last) < 2
        if len(id_last) == 1:
            last_page = int(dico_request['properties']['links'][id_last[0]]['href'].split('=')[-1])+1
            full_url = '='.join(dico_request['properties']['links'][id_last[0]]['href'].split('=')[0:-1]) + '='
            pool = multiprocessing.Pool(max(1,min(20, last_page-1)))
            url_requests = [full_url + str(ii) for ii in range(2,last_page+1)]
            for ii, dico_request in enumerate(pool.map(json_http_request_no_error, url_requests)):
                if dico_request is None:
                    print('request failed: %s, retrying...'%url_requests[ii])
                    dico_request = json_http_request_no_error(url_requests[ii])
                if dico_request is None:
                    print('request failed: %s'%url_requests[ii])
                    continue
                dico.update(features_to_dict(dico_request['features'], properties_selection=properties_selection))
                
        #in case there was a false count and there are still pages left (happens very often), go through them sequentially
        if dico_request is not None:
            while(True):
                id_next = [ii for ii in range(len(dico_request['properties']['links'])) if dico_request['properties']['links'][ii]['rel'] == 'next']
                assert len(id_next) < 2
                if len(id_next) == 0:
                    break
                elif len(id_next) == 1:
                    dico_request = requests.get(dico_request['properties']['links'][id_next[0]]['href']).json()
                    dico.update(features_to_dict(dico_request['features'], tile_id_post_selection=tile_id_post_selection, properties_selection=properties_selection))
                
        if output_file is not None:
            dump_json(dico, output_file)
        
        return dico
        
        

if __name__ == '__main__':
    
    # ~ dico = WekeoCosimsProductParser().search(datetime(2020,6,14), datetime(2020,6,15))
    dico = WekeoCosimsProductParser().get_product_info(['FSC_20200615T102823_S2B_T32TLR_V001_1'])
    set_trace()

    
