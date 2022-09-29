#!/usr/bin/env python3
# -*- coding: utf-8 -*-



import os, sys, shutil, subprocess
import requests
from datetime import datetime, timedelta
import time
import tempfile
import json

default_creodias_credentials = {'user': 'alien@xenomorph.fr', 'password': 'alien@lv426'}
            
def json_http_request(http_request, error_mode=True):
    try:
        output = requests.get(http_request).json()
    except:
        if error_mode:
            raise
        else:
            return None
    return output


def json_http_request_no_error(http_request):
    return json_http_request(http_request, error_mode=False)
    
    
            
def universal_l1c_id(product_id):
    product_id = os.path.basename(product_id) #make sure its just a name and not a path
    assert product_id.split('_')[1] == 'MSIL1C'
    product_id = product_id.replace('.SAFE','') #make sure the '.SAFE' is removed just like in amlthee indexes
    assert [len(el) for el in product_id.split('_')] == [3, 6, 15, 5, 4, 6, 15] #check if syntax matches 'S2A_MSIL1C_20180102T102421_N0206_R065_T32TLR_20180102T123237' format
    _ = datetime.strptime(product_id.split('_')[-5], '%Y%m%dT%H%M%S') #check that the sensor_start_date can indeed be read
    _ = datetime.strptime(product_id.split('_')[-1], '%Y%m%dT%H%M%S') #check that the publication_date can indeed be read
    product_id += '.SAFE'
    return product_id
    
    
    
def universal_l2a_sen2cor_id(product_id):
    product_id = os.path.basename(product_id) #make sure its just a name and not a path
    assert product_id.split('_')[1] == 'MSIL2A'
    product_id = product_id.replace('.SAFE','') #make sure the '.SAFE' is removed just like in amlthee indexes
    assert [len(el) for el in product_id.split('_')] == [3, 6, 15, 5, 4, 6, 15] #check if syntax matches 'S2A_MSIL1C_20180102T102421_N0206_R065_T32TLR_20180102T123237' format
    _ = datetime.strptime(product_id.split('_')[-5], '%Y%m%dT%H%M%S') #check that the sensor_start_date can indeed be read
    _ = datetime.strptime(product_id.split('_')[-1], '%Y%m%dT%H%M%S') #check that the publication_date can indeed be read
    product_id += '.SAFE'
    return product_id
    
    
def universal_s2glc_id(product_id):
    product_id = os.path.basename(product_id) #make sure its just a name and not a path
    assert len(product_id.split('_')) == 3
    assert product_id.split('_')[0] == 'S2GLC'
    assert product_id.split('_')[2] == '2017'
    assert len(product_id.split('_')[1]) == 6
    assert product_id.split('_')[1][0] == 'T'
    return product_id
    


def get_creodias_token(username, password):
    http_request = "curl -s -d 'client_id=CLOUDFERRO_PUBLIC'" + ' -d "username=%s"'%username + ' -d "password=%s"'%password + \
        " -d 'grant_type=password' 'https://auth.creodias.eu/auth/realms/DIAS/protocol/openid-connect/token' --insecure"
    ii = 0
    while(True):
        try:
            out = eval(subprocess.check_output(http_request, shell=True))
            token = out['access_token']
            return token
        except:
            ii += 1
            if ii > 3:
                raise
            time.sleep(5)
        
def convert_dates_in_creodias_dict(dico):
    for key in dico.keys():
        if key in ['startDate', 'completionDate', 'updated', 'published']:
            str_len = len(dico[key])
            if 22 <= str_len <= 27:
                dico[key] = datetime.strptime(dico[key][0:-1] + '0'*(27-str_len) + dico[key][-1], '%Y-%m-%dT%H:%M:%S.%fZ')
            elif str_len == 20:
                dico[key] = datetime.strptime(dico[key], '%Y-%m-%dT%H:%M:%SZ')
            else:
                raise Exception('%s : invalid creodias date format'%dico[key])
    return dico
    

def features_to_dict(features, properties_selection=None, tile_id_post_selection=None):
    if tile_id_post_selection is not None:
        if properties_selection is not None:
            return {feature['properties']['title']: convert_dates_in_creodias_dict({key: feature['properties'][key] for key in properties_selection}) \
                for feature in features if feature['properties']['title'].split('_')[-2][1:] in tile_id_post_selection}
        else:
            return {feature['properties']['title']: convert_dates_in_creodias_dict(feature['properties']) \
                for feature in features if feature['properties']['title'].split('_')[-2][1:] in tile_id_post_selection}
    else:
        if properties_selection is not None:
            return {feature['properties']['title']: convert_dates_in_creodias_dict({key: feature['properties'][key] for key in properties_selection}) for feature in features}
        else:
            return {feature['properties']['title']: convert_dates_in_creodias_dict(feature['properties']) for feature in features}
    
    
    
    
def try_multiple_requests(http_request):
    start_time = time.time()
    ntries, dt = 10, 5.
    i0 = 0
    while(True):
        dico_request = json_http_request_no_error(http_request)
        bad_request = dico_request is None
        if not bad_request:
            if 'features' not in dico_request:
                bad_request = True
        if not bad_request:
            return dico_request
        i0 += 1
        if i0 > ntries:
            break
        time.sleep(dt+np.random.rand())
    if bad_request:
        print(http_request)
        raise Exception('access to DIAS API failure')
    
    
    
    
def get_product_type(product_id):
    
    try:
        universal_id = universal_l1c_id(product_id)
        return 'S2_L1C', universal_id
    except:
        pass
        
    try:
        universal_id = universal_l2a_sen2cor_id(product_id)
        return 'S2_L2A', universal_id
    except:
        pass
        
    try:
        universal_id = universal_s2glc_id(product_id)
        return 'S2_GLC', universal_id
    except:
        pass
        
    return None
    
    
def get_creodias_search_url(product_type):
    if product_type in ['S2_L1C', 'S2_L2A']:
        return 'https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json'
    elif product_type in ['S2_GLC']:
        return 'https://finder.creodias.eu/resto/api/collections/S2GLC/search.json'
    raise Exception('product type %s is not handled'%product_type)

    
def get_product_info(product_id, verbose=0):
    
    if verbose > 0:
        print('Getting product information from creodias for product %s'%product_id)
    time0 = time.time()
    product_type, product_id = get_product_type(product_id)
    if product_type is None:
        raise Exception('product type of product %s is unknown, cannot process'%product_id)
    main_url = get_creodias_search_url(product_type)
    dico_request = try_multiple_requests(main_url + '?' + 'productIdentifier=' + '%25' + product_id + '%25')
    if dico_request is None:
        raise Exception('product %s not found'%product_id)
    dico = features_to_dict(dico_request['features'])
    assert product_id == list(dico.keys())[0], '%s != %s'%(product_id, list(dico.keys())[0])
    
    if verbose > 0:
        print('  -> Product info gathered in %s seconds'%(time.time()-time0))

    return dico
    

def search_creodias_catalog_image(product_type, date_min, date_max, tile_id=None, footprint_wkt=None, tile_id_post_selection=None, properties_selection=None, verbose=0):
    
    assert date_max > date_min
    
    #quick hack to avoid having multiple pages on signle tile id requests : split by years
    if (tile_id is not None) and (date_max - date_min > timedelta(400)):
        date_min_loc, date_max_loc = date_min, min(date_max, date_min+timedelta(400))
        dico = dict()
        while(True):
            dico.update(search_creodias_catalog(date_min_loc, date_max_loc, tile_id=tile_id, footprint_wkt=footprint_wkt, tile_id_post_selection=tile_id_post_selection, \
                properties_selection=properties_selection, verbose=verbose))
            if date_max_loc >= date_max:
                break
            date_min_loc = date_max_loc
            date_max_loc = min(date_max, date_max_loc+timedelta(400))
        return dico
    
    main_url = get_creodias_search_url(product_type)
        
    url_params = dict()
    url_params['maxRecords'] = '2000'
    if product_type == 'S2_L1C':
        url_params['processingLevel'] = 'LEVEL1C'
    elif product_type == 'S2_L2A':
        url_params['processingLevel'] = 'LEVEL2A'
    else:
        raise Exception('unhandled product type %s'%product_type)
    url_params['status'] = 'all'
    url_params['dataset'] = 'ESA-DATASET'
    url_params['sortParam'] = 'startDate'
    url_params['sortOrder'] = 'descending'
    if tile_id is not None:
        url_params['productIdentifier'] = '%25' + tile_id + '%25'
    if footprint_wkt is not None:
        url_params['geometry'] = '{}'.format(footprint_wkt.replace('POLYGON ','POLYGON').replace(', ',',').replace(' ','+'))
    url_params['startDate'] = date_min.strftime('%Y-%m-%dT%H:%M:%SZ')
    url_params['completionDate'] = date_max.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    full_url = main_url + '?' + '&'.join(['%s=%s'%(key, value) for key, value in url_params.items()])
    dico_request = try_multiple_requests(full_url)
    dico = features_to_dict(dico_request['features'], tile_id_post_selection=tile_id_post_selection, properties_selection=properties_selection)
    
    id_last = [ii for ii in range(len(dico_request['properties']['links'])) if dico_request['properties']['links'][ii]['rel'] == 'last']
    assert len(id_last) < 2
    if len(id_last) == 1:
        last_page = int(dico_request['properties']['links'][id_last[0]]['href'].split('=')[-1])+1
        full_url = '='.join(dico_request['properties']['links'][id_last[0]]['href'].split('=')[0:-1]) + '='
        pool = multiprocessing.Pool(max(1,min(20, last_page-1)))
        for dico_request in pool.map(try_multiple_requests, [full_url + str(ii) for ii in range(2,last_page+1)]):
            dico.update(features_to_dict(dico_request['features'], properties_selection=properties_selection))
            
    #in case there was a false count and there are still pages left (happens very often), go through them sequentially
    while(True):
        id_next = [ii for ii in range(len(dico_request['properties']['links'])) if dico_request['properties']['links'][ii]['rel'] == 'next']
        assert len(id_next) < 2
        if len(id_next) == 0:
            break
        elif len(id_next) == 1:
            dico_request = requests.get(dico_request['properties']['links'][id_next[0]]['href']).json()
            dico.update(features_to_dict(dico_request['features'], tile_id_post_selection=tile_id_post_selection, properties_selection=properties_selection))
    
    return dico
    
    
def download_product(product_dict, output_dir, creodias_credentials=None, temp_dir=None, verbose=0):
    
    assert len(product_dict) == 1
    product_id = list(product_dict.keys())[0]
    info_product = list(product_dict.values())[0]
    
    os.makedirs(output_dir, exist_ok=True)
    product_filepath = os.path.join(output_dir, product_id)
    if os.path.exists(product_filepath):
        if verbose > 0:
            print('product %s already exists, skipping'%product_id)
        return
    
    if creodias_credentials is None:
        creodias_credentials = default_creodias_credentials
    if isinstance(creodias_credentials, str):
        assert creodias_credentials.count(':') == 1
        creodias_credentials = {'user': creodias_credentials.split(':')[0], 'password': creodias_credentials.split(':')[1]}
    
    if verbose > 0:
        print('Downloading product %s from creodias'%product_id)
    start_time = time.time()
    #last resort : dias_external_api download, intermittent and unreliable
    ntries = 5
    itry = 0
    status = 'failure'
    temp_dir_session = os.path.abspath(tempfile.mkdtemp(prefix='creodl', dir=temp_dir))
    temp_dir_session_dl = os.path.abspath(tempfile.mkdtemp(prefix='dl', dir=temp_dir_session))
    temp_dir_session_ex = os.path.abspath(tempfile.mkdtemp(prefix='ex', dir=temp_dir_session))
    adress_id = info_product['services']['download']['url']
    dl_filename = '%s.zip'%(adress_id.split('/')[-1])
    dl_filepath = os.path.join(temp_dir_session_dl, dl_filename)

    while(True):
        os.makedirs(temp_dir_session_dl, exist_ok=True)
        try:
            adress = '%s?token=%s'%(adress_id, get_creodias_token(creodias_credentials['user'], creodias_credentials['password']))

            #download
            subprocess.check_call('wget %s -O %s -q'%(adress, dl_filepath), shell=True)

            #unzip
            subprocess.check_call('cd %s; unzip -q %s; rm -f %s'%(temp_dir_session_dl, dl_filename, dl_filename), shell=True)
            files_temp = os.listdir(temp_dir_session_dl)
            if len(files_temp) != 1:
                raise Exception('multiple or no files found after unzip')
            shutil.move(os.path.join(temp_dir_session_dl, files_temp[0]), os.path.join(temp_dir_session_ex, files_temp[0]))
            if verbose > 0:
                print('Product successfully downloaded from DIAS API: %s (%s seconds)'%(files_temp[0], (time.time()-start_time)))
            break
        except:
            itry += 1
            if itry >= ntries:
                raise Exception('  -> dowload failed after %d tries'%ntries)
            else:
                time.sleep(5.*itry)
                if verbose > 0:
                    print(' -> try %d failed, retrying...'%itry)
        finally:
            shutil.rmtree(temp_dir_session_dl)
            

    #check that extracted file corresponds to requested product id and move product to output directory
    assert files_temp[0] == product_id
    shutil.move(os.path.join(temp_dir_session_ex, files_temp[0]), product_filepath)
    shutil.rmtree(temp_dir_session)

    


def download_product_from_creodias(product_id, output_dir, rclone_path=None, config_file=None, creodias_credentials=None, temp_dir=None, verbose=0):
    
    start_time = datetime.utcnow()
    
    os.makedirs(output_dir, exist_ok=True)
    if temp_dir is None:
        temp_dir = os.path.abspath(os.getcwd())
    os.makedirs(temp_dir, exist_ok=True)
    
    #get product information
    product_dict = get_product_info(product_id, verbose=verbose)
    
    #download product
    download_product(product_dict, output_dir, creodias_credentials=creodias_credentials, temp_dir=temp_dir, verbose=verbose)
    



def download_product_from_creodias_between_dates(product_type, date_min, date_max, output_dir, additional_info=None, creodias_credentials=None, temp_dir=None, verbose=0):
    
    if additional_info is None:
        additional_info = dict()
        
    os.makedirs(output_dir, exist_ok=True)
    if temp_dir is None:
        temp_dir = os.path.abspath(os.getcwd())
    os.makedirs(temp_dir, exist_ok=True)
    
    if product_type in ['S2_L1C', 'S2_L2A']:
        necessary_keys = set(['tile_id'])
        assert set(list(additional_info.keys())) == necessary_keys, 'additional_info must contain the following keys: %s'%(', '.join(sorted(list(necessary_keys))))
        dico_products = search_creodias_catalog_image(product_type, date_min, date_max, tile_id=additional_info['tile_id'], verbose=verbose)
    else:
        raise Exception('product type %s not handled'%product_type)
        
    for key in sorted(list(dico_products.keys())):
        download_product({key: dico_products[key]}, output_dir, creodias_credentials=creodias_credentials, temp_dir=temp_dir, verbose=verbose)
        
        
        
        
        
############################
if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description="Download satellite products from Creodias", formatter_class=argparse.RawTextHelpFormatter)
    group_ex = parser.add_mutually_exclusive_group(required=True)
    group_ex.add_argument("--product_id", type=str, help="product ID. product type is deduced.")
    group_ex.add_argument("--product_type", type=str, help="product type. can be of types S2_L1C, S2_L2A or S2_GLC. " + \
        "date_min and date_max arguments are required in this case, as well as additional_info in some cases.")
    parser.add_argument("--date_min", type=str, help="date_min in Y-m-d format")
    parser.add_argument("--date_max", type=str, help="date_max in Y-m-d format")
    parser.add_argument("--additional_info", type=str, help="coma separated key1:arg1,key2:arg2. tile_id is necessary for product types S2 or S1.")
    parser.add_argument("--output_dir", type=str, required=True, help="output directory")
    parser.add_argument("--creodias_credentials", type=str, help="user:password creodias_credentials")
    parser.add_argument("--temp_dir", type=str, help="temp directory")
    parser.add_argument("--verbose", type=int, default=1, help="verbose level")
    args = parser.parse_args()

    if args.product_id is not None:
        download_product_from_creodias(args.product_id, args.output_dir, creodias_credentials=args.creodias_credentials, temp_dir=args.temp_dir, verbose=args.verbose)
    elif args.product_type is not None:
        assert (args.date_min is not None) and (args.date_max is not None)
        args.date_min = datetime.strptime(args.date_min, '%Y-%m-%d')
        args.date_max = datetime.strptime(args.date_max, '%Y-%m-%d')
        additional_info = dict()
        if args.additional_info is not None:
            for el in args.additional_info.split(','):
                key, value = el.split(':')
                additional_info[key] = value
        download_product_from_creodias_between_dates(args.product_type, args.date_min, args.date_max, args.output_dir, \
            additional_info=additional_info, creodias_credentials=args.creodias_credentials, temp_dir=args.temp_dir, verbose=args.verbose)
    
    
    
    
    
