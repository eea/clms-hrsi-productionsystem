#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_software_part2.rlie_s1_processing_chain import rlie_s1_processing_chain
from si_software_part2.rlie_s1_preprocessing_compute_s1s2_intersection import rlie_s1_preprocessing_compute_s1s2_intersection
from si_utils.rclone import Rclone
import yaml

ldir = os.getcwd()

def rlies1_ondias_singles1grd_processing(s1grd, product_output_storage, add_s2_tileid_to_storage_path=False, nprocs=1, temp_dir=None, rlie_s1_static_aux=None, rclone_config_file=None):
    
    print('\nInitializing clone')
    if rclone_config_file is None:
        rclone_config_file = '/home/%s/.config/rclone/rclone.conf'%(os.environ['USER'])
    assert os.path.exists(rclone_config_file)
    rclone_obj = Rclone(config_file=rclone_config_file)
    
    print('\nChecking that all buckets are defined in rclone config')
    listremotes = rclone_obj.listremotes()
    assert 'eodata:' in listremotes
    assert 'bar:' in listremotes
    if ':' in product_output_storage:
        assert product_output_storage.count(':') == 1, 'mutliple ":" in product_output_storage %s, cannot guess bucket name'%product_output_storage
        assert product_output_storage.split(':')[0] + ':' in listremotes, 'bucket %s not found in remote list %s'%(product_output_storage.split(':')[0], ', '.join(listremotes))
    
    print('\nCreating temp dir for processing')
    temp_dir_session = tempfile.mkdtemp(dir=temp_dir, prefix='rlies1main')
    print('  -> %s'%temp_dir_session)
    print('\nInitializing subdirectories...')
    os.makedirs(os.path.join(temp_dir_session, 'static'))
    os.makedirs(os.path.join(temp_dir_session, 'dynamic'))
    os.makedirs(os.path.join(temp_dir_session, 'outputs', 'preprocessing'))
    os.makedirs(os.path.join(temp_dir_session, 'outputs', 'processing'))
    os.makedirs(os.path.join(temp_dir_session, 'temp'))
    
    print('\nCopying S1 GRD product %s to dynamic subfolder'%s1grd)
    date_s1grd = datetime.strptime(s1grd.split('_')[4], '%Y%m%dT%H%M%S')
    rclone_obj.copy('eodata:EODATA/Sentinel-1/SAR/GRD/%s/%s'%(date_s1grd.strftime('%Y/%m/%d'), s1grd), os.path.join(temp_dir_session, 'dynamic', s1grd))
    
    if rlie_s1_static_aux is None:
        print('\nGetting rlie_s1_static_aux from bucket: bar:hidden_value/rlie_s1_static_aux.tar.gz')
        rclone_obj.copy('bar:hidden_value/rlie_s1_static_aux.tar.gz', temp_dir_session)
        rlie_s1_static_aux_arch_loc = os.path.join(temp_dir_session, 'rlie_s1_static_aux.tar.gz')
        assert os.path.exists(rlie_s1_static_aux_arch_loc)
        extract_archive_within_dir(rlie_s1_static_aux_arch_loc, temp_dir_session)
        rlie_s1_static_aux = os.path.join(temp_dir_session, 'rlie_s1_static_aux')
        assert os.path.isdir(rlie_s1_static_aux)
        os.unlink(rlie_s1_static_aux_arch_loc)
        del rlie_s1_static_aux_arch_loc
        
    #######
    print('\nComputing S1 GRD product intersection with S2 tiles')
    rlie_s1_preprocessing_compute_s1s2_intersection(os.path.join(temp_dir_session, 'dynamic', s1grd), \
        os.path.join(rlie_s1_static_aux, 'sentinel2tiles/s2tiles_eea39_gdal_info.json'), \
        os.path.join(temp_dir_session, 'outputs', 'preprocessing', 'precomputed_product_geometries.json'), \
        temp_dir=os.path.join(temp_dir_session, 'temp'))
    with open(os.path.join(temp_dir_session, 'outputs', 'preprocessing', 'precomputed_product_geometries.json')) as ds:
        dem_tiles_load = set(json.load(ds).keys())
    print('  -> Intersection:', sorted(list(dem_tiles_load)))
    #######
    
    print('\nGetting necessary dem tiles for processing')
    for tile_id in dem_tiles_load:
        print('  -> %s'%tile_id)
        rclone_obj.copy('bar:hidden_value/eu_dem/{0}/S2__TEST_AUX_REFDE2_T{0}_0001/S2__TEST_AUX_REFDE2_T{0}_0001.DBL.DIR/dem_20m.tif'.format(tile_id),
            os.path.join(temp_dir_session, 'dynamic', 'dem', tile_id))
        shutil.move(os.path.join(temp_dir_session, 'dynamic', 'dem', tile_id, 'dem_20m.tif'), \
            os.path.join(temp_dir_session, 'dynamic', 'dem', 'dem_20m_%s.tif'%tile_id))
        shutil.rmtree(os.path.join(temp_dir_session, 'dynamic', 'dem', tile_id))
    
    #######
    print('\nLaunching main processing (longest task = up to 2h max)')
    exitcode = rlie_s1_processing_chain(os.path.join(temp_dir_session, 'dynamic', s1grd), os.path.join(temp_dir_session, 'outputs', 'processing'), \
        os.path.join(rlie_s1_static_aux, 'EU_HYDRO/eu_hydro_3035.shp'), os.path.join(rlie_s1_static_aux, 'sentinel2tiles/s2tiles_eea39_gdal_info.json'), \
        os.path.join(rlie_s1_static_aux, 'HRL_FLAGS'), os.path.join(temp_dir_session, 'dynamic', 'dem'), \
        precomputed_product_geometries_file=os.path.join(temp_dir_session, 'outputs', 'preprocessing', 'precomputed_product_geometries.json'), \
        temp_dir=os.path.join(temp_dir_session, 'temp'), \
        nprocs=nprocs, verbose=3, return_with_error_code=True)
    if exitcode == 0:
        print('\nProcessing successful')
    else:
        raise Exception('Processing failed, returned error %s'%exitcode)
    #######
    
    print('\nReading list of products generated')
    with open(os.path.join(temp_dir_session, 'outputs', 'processing', 'data', 'product_dict.yaml')) as ds:
        product_list = sorted(list(yaml.load(ds).values()))
    print('%d products generated:\n%s'%(len(product_list), '\n'.join([' - %s'%el for el in product_list])))
    
    print('\nCopying outputs to %s'%product_output_storage)
    for product in product_list:
        tile_id = product.split('_')[-3][1:]
        date_loc = datetime.strptime(product.split('_')[1], '%Y%m%dT%H%M%S')
        assert date_loc == date_s1grd #in case there is some very weird bug, just to be sure
        target_subdir_loc = date_loc.strftime('%Y/%m/%d')
        if add_s2_tileid_to_storage_path:
            target_subdir_loc = tile_id + '/' + target_subdir_loc
        rclone_obj.copy(os.path.join(temp_dir_session, 'outputs', 'processing', 'data', product), \
            os.path.join(product_output_storage, target_subdir_loc, product))
    
    print('\nClean everything in %s'%temp_dir_session)
    shutil.rmtree(temp_dir_session)
    
    
def check_is_s1grd_filename(s1grd_name):
    if '.SAFE' not in s1grd_name:
        s1grd_name += '.SAFE'
    try:
        assert s1grd_name.split('.')[-1] == 'SAFE'
        assert len(s1grd_name) == 72
    except:
        raise Exception('improper S1GRD name: %s'%s1grd_name)
    return s1grd_name
        

########################################
if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='This script is used to generate RLIE S1 products for a given S1 GRD product. It ')
    parser.add_argument("--s1grd", type=str, required=True, help='s1 grd product name (with or without .SAFE) or path to file with list of s1grd products')
    parser.add_argument("--product_output_storage", type=str, required=True, help='output path to store products (can be remote bucket). Products will be stored in product_output_storage/YYYY/MM/DD or ' + \
        'product_output_storage/s2_tile_id/YYYY/MM/DD if add_s2_tileid_to_storage_path option is active.')
    parser.add_argument("--add_s2_tileid_to_storage_path", action='store_true', help='store products using product_output_storage/s2_tile_id/YYYY/MM/DD')
    parser.add_argument("--nprocs", type=int, help='number of procs, default is 1', default=1)
    parser.add_argument("--temp_dir", type=str, help='temp directory')
    parser.add_argument("--rlie_s1_static_aux", type=str, help='path to rlie_s1_static_aux directory. ' + \
        'If not defined it gets it from bar:hidden_value/rlie_s1_static_aux.tar.gz but you loose download time (few seconds).')
    parser.add_argument("--rclone_config_file", type=str, help='rclone_config_file path. If not defined it will look for it at /home/$USER/.config/rclone/rclone.conf. ' + \
        'Access to bucket bar and eodata must be defined as well as access to any bucket used in product_output_storage')
    args = parser.parse_args()
        
        
        
    if os.path.isfile(args.s1grd):
        #list of s1grd in text file
        with open(args.s1grd) as ds:
            s1grd_list = [el.replace(' ','').replace('\t','') for el in ds.read().split('\n') if len(el.replace(' ','').replace('\t','')) > 10]
            s1grd_list = [check_is_s1grd_filename(el) for el in s1grd_list]
        for product_name in s1grd_list:
            try:
                print('\nLaunching %s'%product_name)
                rlies1_ondias_singles1grd_processing(product_name, args.product_output_storage, add_s2_tileid_to_storage_path=args.add_s2_tileid_to_storage_path, nprocs=args.nprocs, \
                    temp_dir=args.temp_dir, rlie_s1_static_aux=args.rlie_s1_static_aux, rclone_config_file=args.rclone_config_file)
            except Exception as exe:
                print(str(exe))
                print('  -> processing of %s failed'%product_name)
    else:
        #single s1grd product name
        args.s1grd = check_is_s1grd_filename(args.s1grd)
        print('\nLaunching %s'%args.s1grd)
        rlies1_ondias_singles1grd_processing(args.s1grd, args.product_output_storage, add_s2_tileid_to_storage_path=args.add_s2_tileid_to_storage_path, nprocs=args.nprocs, \
            temp_dir=args.temp_dir, rlie_s1_static_aux=args.rlie_s1_static_aux, rclone_config_file=args.rclone_config_file)

