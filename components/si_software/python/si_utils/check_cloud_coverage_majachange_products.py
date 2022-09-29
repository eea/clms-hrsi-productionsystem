#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_geometry.geometry_functions import *
from si_utils.rclone import Rclone
from product_request_and_download.parse_s2_products_wekeo import WekeoS2ProductParser

def check_cloud_coverage_on_majachange_products(input_file, output_dir, rclone_config_file):
    
    #read FSC product list
    with open(input_file) as ds:
        fsc_products = ds.read().split('\n')
    fsc_products = [os.path.basename(el) for el in fsc_products if 'FSC' in el]
    
    copy_fsc_dir = os.path.join(output_dir, 'FSC')
    os.makedirs(copy_fsc_dir, exist_ok=True)
    rclone_cmd = Rclone(config_file=rclone_config_file)
    wekeo_parser = WekeoS2ProductParser()
    for fsc_product in fsc_products:
        print('%s :'%fsc_product)
        tile_id = fsc_product.split('_')[-3][1:]
        date_loc = datetime.strptime(fsc_product.split('_')[-5], '%Y%m%dT%H%M%S')
        input_fsc_file_loc = 'cosims-results:HRSI/CLMS/Pan-European/High_Resolution_Layers/Snow/FSC/' + \
            date_loc.strftime('%Y/%m/%d') + '/' + fsc_product + '/' + fsc_product + '_FSCTOC.tif'
        output_fsc_dir_loc = os.path.join(copy_fsc_dir, fsc_product)
        if not os.path.exists(output_fsc_dir_loc):
            print(' - %s -> %s'%(input_fsc_file_loc, output_fsc_dir_loc))
            rclone_cmd.copy(input_fsc_file_loc, output_fsc_dir_loc)
        cloud_info_file = os.path.join(output_fsc_dir_loc, 'cloud_info.json')
        if not os.path.exists(cloud_info_file):
            dico_cloud = dict()
            ar_loc = np.ma.masked_invalid(read_band(os.path.join(output_fsc_dir_loc, fsc_product + '_FSCTOC.tif')))
            ar_loc.mask[ar_loc == 255] = True
            n_pixels_cloud = np.sum(ar_loc == 205)
            n_pixels_nodata = np.sum(ar_loc.mask)
            n_total = np.prod(np.shape(ar_loc))
            dico_cloud['cloud_vs_valid'] = n_pixels_cloud*100./(n_total-n_pixels_nodata)
            dico_cloud['cloud_vs_all'] = n_pixels_cloud*100./n_total
            dico_cloud['noclearsky_vs_all'] = (n_pixels_cloud + n_pixels_nodata)*100./n_total
            l1c_dict = wekeo_parser.search(date_loc - timedelta(hours=1), date_loc + timedelta(hours=1), tile_id=tile_id, \
                properties_selection=['productIdentifier', 'cloudCover'])
            assert len(l1c_dict) == 1
            dico_cloud['l1c_cloud_ratio'] = list(l1c_dict.values())[0]['cloudCover']
        else:
            dico_cloud = load_json(cloud_info_file)
        print('\n'.join([' - %s: %s%%'%(key, val) for key, val in dico_cloud.items()]))
    
    

if __name__ == '__main__':
    
    
    import argparse
    parser = argparse.ArgumentParser(description="check cloud coverage on majachange_products", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--input_file", type=str, required=True, help="path to input file containing FSC and RLIE files")
    parser.add_argument("--output_dir", type=str, required=True, help="output directory")
    parser.add_argument("--rclone_config_file", type=str, required=True, help="rclone_config_file")
    args = parser.parse_args()
    
    check_cloud_coverage_on_majachange_products(args.input_file, args.output_dir, args.rclone_config_file)
