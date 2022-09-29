#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""chain processing on DIAS"""

from si_geometry.geometry_functions import *
from datetime import datetime, timedelta
import multiprocessing


main_s1_search_url = 'https://finder.creodias.eu/resto/api/collections/Sentinel1/search.json'

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
    

def features_to_dict(features, properties_selection=None, tile_id_post_selection=None):
    if tile_id_post_selection is not None:
        if properties_selection is not None:
            return {feature['properties']['title']: {'geometry': feature['geometry'], 'properties': convert_dates_in_wekeo_dict({key: feature['properties'][key] for key in properties_selection})} \
                for feature in features if feature['properties']['title'].split('_')[-2][1:] in tile_id_post_selection}
        else:
            return {feature['properties']['title']: {'geometry': feature['geometry'], 'properties': convert_dates_in_wekeo_dict(feature['properties'])} \
                for feature in features if feature['properties']['title'].split('_')[-2][1:] in tile_id_post_selection}
    else:
        if properties_selection is not None:
            return {feature['properties']['title']: {'geometry': feature['geometry'], \
                'properties': convert_dates_in_wekeo_dict({key: feature['properties'][key] for key in properties_selection})} for feature in features}
        else:
            return {feature['properties']['title']: {'geometry': feature['geometry'], 'properties': convert_dates_in_wekeo_dict(feature['properties'])} for feature in features}
    
    


        
        



def get_s1_product_info_wekeo(product_list, properties_selection=None, verbose=0, error_mode=True, output_file=None):
    
    monooutput = False
    if isinstance(product_list, str):
        product_list = [product_list]
        monooutput = True
    assert isinstance(product_list, list)
    assert len(product_list) > 0
    time0 = time.time()
    
    dico = dict()
    pool = multiprocessing.Pool(min(20, len(product_list)))
    for dico_request in pool.map(try_multiple_requests, [main_s1_search_url + '?' + 'productIdentifier=' + '%25' + product_id + '%25' for product_id in product_list]):
        if dico_request is not None:
            dico.update(features_to_dict(dico_request['features'], properties_selection=properties_selection))
    
    if set(product_list) != set(list(dico.keys())):
        print('The following products were not retrieved :\n%s\n'%('\n'.join(['- %s'%el for el in sorted(list(set(product_list) - set(list(dico.keys()))))])))
        print('The following products were retrieved but do not match requested product ids:\n%s\n'%('\n'.join(['- %s'%el for el in sorted(list(set(list(dico.keys())) - set(product_list)))])))
        if error_mode:
            raise Exception('product request and retrieval mismatch')
    
    if verbose > 0:
        print('  -> Executed in %s seconds'%(time.time()-time0))
        
    if monooutput:
        dico = dico[product_list[0]]
        
    if output_file is not None:
        dump_json(dico, output_file)

    return dico
        

    
def search_s1_wekeo(date_min, date_max, footprint_wkt=None, properties_selection=None, output_file=None, verbose=0):
    
    assert date_max > date_min
        
    url_params = dict()
    url_params['maxRecords'] = '2000'
    url_params['productType'] = 'GRD'
    url_params['sensorMode'] = 'IW'
    url_params['polarisation'] = 'VV+VH'
    url_params['status'] = 'all'
    url_params['dataset'] = 'ESA-DATASET'
    url_params['sortParam'] = 'published'
    url_params['sortOrder'] = 'ascending'
    if footprint_wkt is not None:
        url_params['geometry'] = '{}'.format(footprint_wkt.replace('POLYGON ','POLYGON').replace(', ',',').replace(' ','+'))
    url_params['startDate'] = date_min.strftime('%Y-%m-%dT%H:%M:%SZ')
    url_params['completionDate'] = date_max.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    full_url = main_s1_search_url + '?' + '&'.join(['%s=%s'%(key, value) for key, value in url_params.items()])
    dico_request = try_multiple_requests(full_url)
    dico = features_to_dict(dico_request['features'], properties_selection=properties_selection)
    
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
            dico.update(features_to_dict(dico_request['features'], properties_selection=properties_selection))
            
    if output_file is not None:
        dump_json(dico, output_file)
    
    return dico
    
    
    
def search_s1_wekeo_eea39(date_min, date_max, eea39_simplified_shape=None, s2_shp_eea39=None, properties_selection=None, output_file=None, verbose=0):
    
    #get footprint_wkt
    footprint_wkt = polygonshape_to_wkt(eea39_simplified_shape)

    #search
    dico = search_s1_wekeo(date_min, date_max, footprint_wkt=footprint_wkt, properties_selection=properties_selection, verbose=verbose)
    
    #post-selection
    if s2_shp_eea39 is not None:
        if isinstance(s2_shp_eea39, str):
            with fiona.open(s2_shp_eea39) as ds:
                try:
                    s2shape_dict = {inv[1]['properties']['tile_name']: shape(inv[1]['geometry']) for inv in list(ds.items())}
                except:
                    s2shape_dict = {inv[1]['properties']['Name']: shape(inv[1]['geometry']) for inv in list(ds.items())}
        else:
            s2shape_dict = s2_shp_eea39
        dico_s2_intersection = dict()
        for i0, (key, value) in enumerate(dico.items()):
            if verbose >= 1:
                print('%d/%d: %s'%(i0+1, len(dico), key))
            dico[key]['s2tiles_intersect_list'] = []
            geom_loc = shape(value['geometry'])
            for tile_id, poly in s2shape_dict.items():
                if poly.intersects(geom_loc):
                    dico[key]['s2tiles_intersect_list'].append(tile_id)
            dico[key]['s2tiles_intersect_list'] = sorted(dico[key]['s2tiles_intersect_list'])
        dico = {key: value for key in dico.keys() if len(dico[key]['s2tiles_intersect_list']) > 0}
        
            
    if output_file is not None:
        dump_json(dico, output_file)
        if verbose >= 1:
            print('Written to %s'%output_file)
            
    return dico

    
    

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description="S1 GRD downloader", formatter_class=argparse.RawTextHelpFormatter)
    
    #search using a product ID
    parser.add_argument("--product_id", type=str, help="get information about a specific product")
    
    #search using time range and geographical zone selection
    parser.add_argument("--start_date", type=str, help="search for products start_date in YYYY-mm-ddTHH:MM:SS format")
    parser.add_argument("--end_date", type=str, help="search for products end_date in YYYY-mm-ddTHH:MM:SS format")
    parser.add_argument("--eea39_simplified_shape", type=str, help="EEA39 simplified shapefile for zone search restriction")
    parser.add_argument("--s2_shp_eea39", type=str, help="s2 tile shape on EEA39 for zone selection after search")

    #write outputs to file
    parser.add_argument("--output_file", type=str, help="output file")
    parser.add_argument("--verbose", type=int, default=1, help="verbose level")
    args = parser.parse_args()
    
    if args.product_id is not None:
        assert all([el is None for el in [args.start_date, args.end_date, args.s2_shp_eea39]])
        get_s1_product_info_wekeo(args.product_id, output_file=args.output_file, verbose=args.verbose)
    else:
        assert args.product_id is None
        args.start_date = datetime.strptime(args.start_date, '%Y-%m-%dT%H:%M:%S')
        args.end_date = datetime.strptime(args.end_date, '%Y-%m-%dT%H:%M:%S')
        search_s1_wekeo_eea39(args.start_date, args.end_date, eea39_simplified_shape=args.eea39_simplified_shape, s2_shp_eea39=args.s2_shp_eea39, \
            output_file=args.output_file, verbose=args.verbose)


    
