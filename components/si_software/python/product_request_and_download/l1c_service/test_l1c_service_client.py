#!/usr/bin/env python3
# -*- coding: utf-8 -*-



from si_common.common_functions import *
import socket
import select
from product_request_and_download.l1c_service.l1c_service_client import download_l1c

        
def test(ndl_max=6, nprocs=1, temp_dir=None, config_file=None, verbose=0):
    test_l1cs_creodias_extraction = """
eodata:EODATA/Sentinel-2/MSI/L2A/2021/02/04/S2B_MSIL2A_20210204T115219_N0214_R123_T28SEA_20210204T142150.SAFE
"""
    test_l1cs = sorted(list(set([el.split('/')[-1] for el in test_l1cs_creodias_extraction.split('\n') if '.SAFE' in el])))
    if ndl_max is not None:
        if len(test_l1cs) > ndl_max:
            test_l1cs = test_l1cs[0:ndl_max]
    import multiprocessing.dummy
    if temp_dir is None:
        temp_dir = os.path.abspath(os.getcwd())
    os.makedirs(temp_dir, exist_ok=True)
    temp_dir_session = tempfile.mkdtemp(prefix='dl_test', dir=temp_dir)
    pool = multiprocessing.dummy.Pool(processes=nprocs)
    pool.map(generic_function, [{'function': download_l1c, 'args': [el, os.path.join(temp_dir_session, 'out')], \
        'kwargs': {'temp_dir': os.path.join(temp_dir_session, 'temp'), 'config_file': config_file, 'verbose': verbose}} for el in test_l1cs])
    # ~ print('test finished, removing %s'%temp_dir_session)
    # ~ shutil.rmtree(temp_dir_session)
    


        
############################
if __name__ == '__main__':
    

    
    import argparse
    parser = argparse.ArgumentParser(description="download_through_dias_l1c_service", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--ndl_max", type=int, default=6, help="maximum number of downloads")
    parser.add_argument("--nprocs", type=int, default=1, help="nprocs")
    parser.add_argument("--temp_dir", type=str, help="temporary directory")
    parser.add_argument("--config_file", type=str, help="rclone config file if different than ~/.config/rclone/rclone.config")
    parser.add_argument("--verbose", type=int, default=1, help="verbose level")
    args = parser.parse_args()

    test(ndl_max=args.ndl_max, nprocs=args.nprocs, temp_dir=args.temp_dir, config_file=args.config_file, verbose=args.verbose)
    
    
    
    
    
    
