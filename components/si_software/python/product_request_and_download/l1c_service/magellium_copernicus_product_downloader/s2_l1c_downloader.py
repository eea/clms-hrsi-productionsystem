#!/usr/bin/env python3
# -*- coding: utf-8 -*-

######################
#developped by Magellium SAS
#author remi.jugier@magellium.fr
######################


from download_copernicus_product import *
import multiprocessing
import json
import numpy as np
import time

cloud_cover_max_default = 90

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

    
        
def convert_dates_in_wekeo_dict(dico):
    for key in dico.keys():
        if key in ['startDate', 'completionDate', 'updated', 'published']:
            str_len = len(dico[key])
            if 22 <= str_len <= 27:
                dico[key] = datetime.strptime(dico[key][0:-1] + '0'*(27-str_len) + dico[key][-1], '%Y-%m-%dT%H:%M:%S.%fZ')
            elif str_len == 20:
                dico[key] = datetime.strptime(dico[key], '%Y-%m-%dT%H:%M:%SZ')
            else:
                raise Exception('%s : invalid wekeo date format'%dico[key])
    return dico
    

def features_to_dict(features):
    return {feature['properties']['title']: convert_dates_in_wekeo_dict(feature['properties']) for feature in features}
    

def search_s2_l1c_products_on_dias(tile_id, date_min, date_max, cloud_cover_max=None, verbose=0):
    
    if cloud_cover_max is None:
        cloud_cover_max = cloud_cover_max_default
    cloud_cover_max = int(cloud_cover_max)
    
    if tile_id[0] == 'T':
        tile_id = tile_id[1:]
    assert date_max > date_min
    
    #split requests by years to avoid reaching the max number of records
    if date_max - date_min > timedelta(365):
        date_min_loc, date_max_loc = date_min, min(date_max, date_min+timedelta(365))
        dico = dict()
        while(True):
            dico.update(search_s2_l1c_products_on_dias(tile_id, date_min_loc, date_max_loc, cloud_cover_max=cloud_cover_max, verbose=verbose))
            if date_max_loc >= date_max:
                break
            date_min_loc = date_max_loc
            date_max_loc = min(date_max, date_max_loc+timedelta(365))
        return dico
    
    main_url = 'https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json'
    url_params = dict()
    url_params['maxRecords'] = '2000'
    url_params['processingLevel'] = 'LEVEL1C'
    url_params['productIdentifier'] = '%25' + tile_id + '%25'
    url_params['status'] = 'all'
    url_params['dataset'] = 'ESA-DATASET'
    url_params['sortParam'] = 'startDate'
    url_params['sortOrder'] = 'descending'
    url_params['startDate'] = date_min.strftime('%Y-%m-%dT%H:%M:%SZ')
    url_params['completionDate'] = date_max.strftime('%Y-%m-%dT%H:%M:%SZ')
    if cloud_cover_max < 100:
        url_params['cloudCover'] = '%5B0%2C' + '%d'%cloud_cover_max + '%5D'
    
    full_url = main_url + '?' + '&'.join(['%s=%s'%(key, value) for key, value in url_params.items()])
    dico_request = try_multiple_requests(full_url)
    dico = features_to_dict(dico_request['features'])
    
    id_last = [ii for ii in range(len(dico_request['properties']['links'])) if dico_request['properties']['links'][ii]['rel'] == 'last']
    assert len(id_last) < 2
    if len(id_last) == 1:
        last_page = int(dico_request['properties']['links'][id_last[0]]['href'].split('=')[-1])+1
        full_url = '='.join(dico_request['properties']['links'][id_last[0]]['href'].split('=')[0:-1]) + '='
        pool = multiprocessing.Pool(max(1,min(20, last_page-1)))
        for dico_request in pool.map(try_multiple_requests, [full_url + str(ii) for ii in range(2,last_page+1)]):
            dico.update(features_to_dict(dico_request['features']))
            
    #in case there was a false count and there are still pages left (happens very often), go through them sequentially
    while(True):
        id_next = [ii for ii in range(len(dico_request['properties']['links'])) if dico_request['properties']['links'][ii]['rel'] == 'next']
        assert len(id_next) < 2
        if len(id_next) == 0:
            break
        elif len(id_next) == 1:
            dico_request = requests.get(dico_request['properties']['links'][id_next[0]]['href']).json()
            dico.update(features_to_dict(dico_request['features']))
    
    return dico
    


def s2_l1c_downloader(tile_id, date_min, date_max, output_dir, server_ip, server_port, bucket_share_path=None, rclone_path=None, config_file=None, temp_dir=None, cloud_cover_max=None, verbose=0):
    product_ids = list(search_s2_l1c_products_on_dias(tile_id, date_min, date_max, cloud_cover_max=cloud_cover_max, verbose=0).keys())
    if len(product_ids) == 0:
        print('No product have been identified withing this time window')
        return
    print('The following products have been identified and will be downloaded:\n%s\n'%('\n'.join(product_ids)))
    for product_id in product_ids:
        print('Requesting %s ...'%product_id)
        download_copernicus_product(product_id, output_dir, server_ip, server_port, bucket_share_path=bucket_share_path, rclone_path=rclone_path, config_file=config_file, \
            temp_dir=temp_dir, verbose=verbose)
    


        
############################
if __name__ == '__main__':
    

    
    import argparse
    parser = argparse.ArgumentParser(description="download Sentinel-3 SRAL level 1 and 2 products through Magellium service on Wekeo", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--product_id", type=str, help="product id. the product id can be given directly. In this case, the product_type, date_min and date_max parameters should not be filled")
    parser.add_argument("--tile_id", type=str, help="tile_id")
    parser.add_argument("--date_min", type=str, help="min start date of the products in Y-m-dTH:M:S format")
    parser.add_argument("--date_max", type=str, help="max start date of the products in Y-m-dTH:M:S format")
    parser.add_argument("--cloud_cover_max", type=int, help="max cloud cover, default is %s"%cloud_cover_max_default, default=cloud_cover_max_default)
    parser.add_argument("--output_dir", type=str, required=True, help="output directory")
    parser.add_argument("--server_ip", type=str, required=True, help="server ip")
    parser.add_argument("--server_port", type=str, required=True, help="server port")
    parser.add_argument("--bucket_share_path", type=str, default=default_bucket_share_path, help="bucket share path. default: %s"%default_bucket_share_path)
    parser.add_argument("--config_file", type=str, help="rclone config file if different than ~/.config/rclone/rclone.config")
    parser.add_argument("--temp_dir", type=str, help="temp directory")
    parser.add_argument("--verbose", type=int, default=1, help="verbose level")
    args = parser.parse_args()
    
    if args.product_id is not None:
        assert args.tile_id is None
        assert args.date_min is None
        assert args.date_max is None
        
        download_copernicus_product(args.product_id, args.output_dir, args.server_ip, args.server_port, bucket_share_path=args.bucket_share_path, \
            config_file=args.config_file, temp_dir=args.temp_dir, verbose=args.verbose)
    else:
        assert args.tile_id is not None
        assert args.date_min is not None
        assert args.date_max is not None
        args.date_min = datetime.strptime(args.date_min, '%Y-%m-%dT%H:%M:%S')
        args.date_max = datetime.strptime(args.date_max, '%Y-%m-%dT%H:%M:%S')
        s2_l1c_downloader(args.tile_id, args.date_min, args.date_max, args.output_dir, args.server_ip, args.server_port, bucket_share_path=args.bucket_share_path, \
            config_file=args.config_file, temp_dir=args.temp_dir, cloud_cover_max=args.cloud_cover_max, verbose=args.verbose)
    
    
    
    
    
    
