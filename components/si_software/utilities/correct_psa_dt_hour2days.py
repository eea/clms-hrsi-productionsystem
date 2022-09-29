#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, shutil, subprocess
import numpy as np
import json

def correct_psa_dt_hour2days(psa_path):
    
    psa_path = os.path.abspath(psa_path)
    
    psa_id_split = os.path.basename(psa_path).split('_')
    if len(psa_id_split) != 5:
        print('skipping %s, not a PSA product'%os.path.basename(psa_path))
        return
    if psa_id_split[0] != 'PSA' or psa_id_split[2] != 'S2' or len(psa_id_split[-2]) != 6 or psa_id_split[-1] != 'V100':
        print('skipping %s, not a PSA product'%os.path.basename(psa_path))
        return
    if len(psa_id_split[1].split('-')[0]) != 8 or (len(psa_id_split[1].split('-')[1]) not in [3,4]):
        raise Exception('skipping %s, not a recognised PSA product'%os.path.basename(psa_path))
    if len(psa_id_split[1].split('-')[1]) == 3:
        print('skipping %s, is a proper PSA product'%os.path.basename(psa_path))
        return
        
    old_product_tag = os.path.basename(psa_path)
    dt = int(np.ceil(float(psa_id_split[1].split('-')[1])/24.))
    new_product_tag = old_product_tag.replace(psa_id_split[1], psa_id_split[1].split('-')[0] + '-%03d'%dt)
    
    #correct file names
    for el in os.listdir(psa_path):
        old_path = os.path.join(psa_path, el)
        new_path = os.path.join(psa_path, el.replace(old_product_tag, new_product_tag))
        if new_path != old_path:
            shutil.move(old_path, new_path)
            
    #correct json
    with open(os.path.join(psa_path, 'dias_catalog_submit.json')) as ds:
        dico_json = json.load(ds)
    dico_json['resto']['properties']['productIdentifier'] = new_product_tag
    dico_json['resto']['properties']['title'] = new_product_tag
    with open(os.path.join(psa_path, 'dias_catalog_submit.json'), mode='w') as ds:
        json.dump(dico_json, ds, indent=4)

    #change main folder path
    new_psa_path = os.path.join(os.path.dirname(psa_path), new_product_tag)
    shutil.move(psa_path, new_psa_path)
    print('processed %s -> %s'%(psa_path, new_psa_path))


if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description='This script is used to correct PSA paths for LAEA products that contained dt in hour instead of days')
    parser.add_argument("psa_path", type=str, help='psa_path')
    args = parser.parse_args()
    
    
    correct_psa_dt_hour2days(args.psa_path)

