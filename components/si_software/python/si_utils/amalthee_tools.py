#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys, shutil
from datetime import datetime, timedelta
import time
import multiprocessing

try:
    from libamalthee import Amalthee
    amalthee_accessible = True
except:
    amalthee_accessible = False
    

def universal_l1c_id(product_id):
    product_id = os.path.basename(product_id) #make sure its just a name and not a path
    assert product_id.split('_')[1] == 'MSIL1C'
    product_id = product_id.replace('.SAFE','') #make sure the '.SAFE' is removed just like in amlthee indexes
    assert [len(el) for el in product_id.split('_')] == [3, 6, 15, 5, 4, 6,
                                                         15]  # check if syntax matches 'S2A_MSIL1C_20180102T102421_N0206_R065_T32TLR_20180102T123237' format
    _ = datetime.strptime(product_id.split('_')[-5], '%Y%m%dT%H%M%S') #check that the sensor_start_date can indeed be read
    _ = datetime.strptime(product_id.split('_')[-1], '%Y%m%dT%H%M%S') #check that the publication_date can indeed be read
    return product_id
    

def get_l1c_filename_info(l1c_filename):
    product_id = universal_l1c_id(l1c_filename)
    return {'tile_id': product_id.split('_')[-2][1:], \
        'sensor_start_date': datetime.strptime(product_id.split('_')[-5], '%Y%m%dT%H%M%S'), \
        'publication_date': datetime.strptime(product_id.split('_')[-1].split('.')[0], '%Y%m%dT%H%M%S')}



class AmaltheeManager:
    
    def __init__(self):
        self.accessible = amalthee_accessible
        
    def __get_args_only(self, product_id, output_dir, timeout, allow_request):
        return self.get(self, product_id, output_dir, timeout=timeout, allow_request=allow_request)
        
    def request_timeserie(tile_id, date_min, date_max):
        try:
            app = Amalthee()
            print('  -> call to amalthee for time series, search and fill')
            app.search('S2ST', date_min.strftime('%Y-%m-%d'), (date_max+timedelta(1)).strftime('%Y-%m-%d'), \
                {'tileid': tile_id, 'processingLevel': 'LEVEL1C'}, nthreads = 1)
            app.fill_datalake()
        except Exception as exe:
            print(str(exe))
        except:
            print('call to amalthee .fill_datalake() prodedure failed')
        
        
    def get(self, product_id, output_dir, timeout=60., allow_request=True):
        if isinstance(product_id, list):
            pool = multiprocessing.Pool(min(10, len(product_id)))
            return pool.starmap(self.__get_args_only, [(el, output_dir, timeout, allow_request) for el in product_id])
        if not self.accessible:
            return 'no_amalthee_access', None
        product_id = universal_l1c_id(product_id)
        dico_loc = get_l1c_filename_info(product_id)
        try:
            print('  -> call to amalthee for single product, fill only')
            app = Amalthee()
            print('  -> call to amalthee for single product, search only')
            app.search('S2ST', dico_loc['sensor_start_date'].strftime('%Y-%m-%d'), (dico_loc['sensor_start_date']+timedelta(1)).strftime('%Y-%m-%d'), \
                {'tileid': dico_loc['tile_id'], 'processingLevel': 'LEVEL1C'}, nthreads = 1)
            if product_id not in app.products.index.tolist():
                return 'product_unknown_to_db', None
            
            if not app.products.available[product_id]:
                if not allow_request:
                    return 'unavailable', None
                app.fill_datalake()
                time.sleep(14)
                start_time = time.time()
                while(True):
                    print('  -> checking datalake for product...')
                    app.check_datalake()
                    if app.products.available[product_id]:
                        break
                    if timeout is not None:
                        if time.time()-start_time > timeout:
                            break
                    time.sleep(14)
            
            if app.products.available[product_id]:
                product_path = app.products.datalake[product_id]
                assert os.path.exists(product_path)
                assert universal_l1c_id(product_path) == product_id
                os.makedirs(output_dir, exist_ok=True)
                target_path = os.path.join(output_dir, os.path.basename(product_path))
                shutil.copytree(product_path, target_path)
                return 'available', target_path
        except Exception as exe:
            print(str(exe))
            return 'unavailable', None
        except:
            print('  -> call to amalthee .fill_datalake() prodedure failed')
            return 'unavailable', None
            
        return 'unavailable', None
