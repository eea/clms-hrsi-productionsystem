#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, shutil, subprocess
import numpy as np
import json

def correct_psa_xml(psa_path):
    
    psa_path = os.path.abspath(psa_path)
    product_id = os.path.basename(psa_path)
    psa_id_split = product_id.split('_')
    if len(psa_id_split) != 5:
        print('skipping %s, not a PSA product'%product_id)
        return
    if psa_id_split[0] != 'PSA' or psa_id_split[2] != 'S2' or len(psa_id_split[-2]) != 6 or psa_id_split[-1] != 'V100':
        print('skipping %s, not a PSA product'%product_id)
        return
    if len(psa_id_split[1].split('-')[0]) != 8 or (len(psa_id_split[1].split('-')[1]) not in [3]):
        raise Exception('skipping %s, not a recognised PSA product'%product_id)
        
    product_id = os.path.basename(psa_path)
    xml_path = os.path.join(psa_path, product_id + '_MTD.xml')
    assert os.path.exists(xml_path)
    with open(xml_path) as ds:
        lines = ds.read().split('\n')
    for ii in range(len(lines)):
        if 'PSA_' in lines[ii] and '_V100' in lines[ii]:
            old_id = 'PSA_' + lines[ii].split('PSA_')[1].split('_V100')[0] + '_V100'
            lines[ii] = lines[ii].replace(old_id, product_id)
    with open(xml_path, mode='w') as ds:
        ds.write('\n'.join(lines))


if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description='This script is used to make the xml file coherent with product ID')
    parser.add_argument("psa_path", type=str, help='psa_path')
    args = parser.parse_args()
    
    
    correct_psa_xml(args.psa_path)

