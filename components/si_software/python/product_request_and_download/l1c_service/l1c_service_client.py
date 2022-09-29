#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
download_through_dias_vm_intermediary uses a service that reads requests for L1Cs on a bucket and makes them available on the same bucket
This is necessary for reprocessing on HAL because both the standard DIAS retrieval API and the CNES amalthee retrieval service are unrieliable.
The service works in a very rudimentary way, simply by exchanging files on bucket (at adress_share_requests).

Client : 
1) Adds a 'product_ids' file containing the list of L1C files requested to a new folder at 'adress_share_requests'.
  To ensure that the server does not read an incomplete file, when it finished copying the 'product_ids' file, it copies a 'copyok' file.
  
Server (this service) :
1) The server scans all directories in 'adress_share_requests' :
  1.1) It erases those where L1C files were retrieved by the client (contains the 'retrieved' file), 
    or those that where the most recent L1C was made available more than 3 hours ago.
  1.2) It skips directories where no 'copyok' or 'product_ids' file is found
  1.3) In a directory containing a 'copyok' and 'product_ids' file it reads the 'product_ids' file that contains a list of L1C files.
    1.3.1) If the L1C file already exists and has been properly copied (contains a 'copyok' file), it skips
    1.3.2) Else it copies the L1C file to the directory from eodata
  
"""

from si_common.common_functions import *
from si_utils.rclone import Rclone
from product_request_and_download.l1c_service.common_variables import adress_share_products, server_ip, server_port_client, cosims_identifier
import requests





def download_l1c(product_id, output_dir, rclone_path=None, config_file=None, temp_dir=None, verbose=0):
    
    start_time = datetime.utcnow()
    
    os.makedirs(output_dir, exist_ok=True)
    if temp_dir is None:
        temp_dir = os.path.abspath(os.getcwd())
    os.makedirs(temp_dir, exist_ok=True)
    
    if os.path.basename(product_id).split('_')[1] == 'MSIL1C':
        product_id = universal_l1c_id(product_id)
    elif os.path.basename(product_id).split('_')[1] == 'MSIL2A':
        product_id = universal_l2a_sen2cor_id(product_id)
    else:
        raise Exception('product type of %s not identified'%product_id)
    rcone_util = Rclone(rclone_path=rclone_path, config_file=config_file)
    
    if verbose >= 2:
        print('Requesting %s to %s:%s'%(product_id, server_ip, server_port_client))
    response = requests.get(os.path.join('http://%s:%s'%(server_ip, server_port_client), cosims_identifier + product_id)).json()
    if verbose >= 2:
        print('Received response from %s:%s -> %s'%(server_ip, server_port_client, dump_json(response)))
    
    
    if response['status'] == 'bucket':
        os.makedirs(temp_dir, exist_ok=True)
        temp_dir_session = tempfile.mkdtemp(prefix='dl_loc', dir=temp_dir)
        try:
            rcone_util.copy(os.path.join(adress_share_products, response['token'], product_id + '.SAFE.tar'), temp_dir_session)
            extract_archive_within_dir(os.path.join(temp_dir_session, product_id + '.SAFE.tar'), output_dir)
            assert os.path.exists(os.path.join(output_dir, product_id + '.SAFE'))
            assert os.path.isdir(os.path.join(output_dir, product_id + '.SAFE'))
        except:
            if os.path.exists(os.path.join(output_dir, product_id + '.SAFE')):
                shutil.rmtree(os.path.join(output_dir, product_id + '.SAFE'))
            raise
        finally:
            shutil.rmtree(temp_dir_session)
            try:
                rcone_util.rmtree(os.path.join(adress_share_products, response['token']))
            except:
                print('Could not remove remote directory: %s'%os.path.join(adress_share_products, response['token']))
    else:
        if verbose >= 1:
            print('Could not retrieve product %s, got status %s'%(product_id, response['status']))
        return None
    if verbose >= 1:
        print('Product %s downloaded from COSIMS product service through %s protocol in %s'%(product_id, response['status'], datetime.utcnow()-start_time))
            

        
############################
if __name__ == '__main__':
    

    
    import argparse
    parser = argparse.ArgumentParser(description="download_through_dias_l1c_service", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--product_id", type=str, required=True, help="S2 product ID")
    parser.add_argument("--output_dir", type=str, required=True, help="output directory")
    parser.add_argument("--config_file", type=str, help="rclone config file if different than ~/.config/rclone/rclone.config")
    parser.add_argument("--temp_dir", type=str, help="temp directory")
    parser.add_argument("--verbose", type=int, default=1, help="verbose level")
    args = parser.parse_args()

    download_l1c(args.product_id, args.output_dir, config_file=args.config_file, temp_dir=args.temp_dir, verbose=args.verbose)
    
    
    
    
    
    
