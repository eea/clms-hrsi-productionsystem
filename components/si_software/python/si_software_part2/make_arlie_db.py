#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, shutil, subprocess, tempfile
from datetime import datetime, timedelta
import multiprocessing
import json

port_number_base = 5432


def launch_db(arlie_metadata_dir, temp_dir_session, docker_file_dir):
    #port_number = port_number_base
    #os.makedirs(temp_dir_session, exist_ok=True)
    
    #cwd = os.path.abspath(os.getcwd())
    #postgresql_docker_name = 'cosims-postgis'

    #create and launch DB
    print('Creating and launching DB...')
    #os.chdir(temp_dir_session)
    #cmd = 'unzip -qq %s'%(os.path.join(arlie_metadata_dir, 'postgresql.zip'))
    #print(cmd)
    #subprocess.check_call(cmd, shell=True)
    
    #change default postgresql port
    txt_psql_conf = open('postgresql/postgresql.conf').read().replace('#port = 5432', 'port = %d'%port_number)
    with open('postgresql/postgresql.conf', mode='w') as ds:
        ds.write(txt_psql_conf)
        ds.write()

    os.chdir(docker_file_dir)
    cmd = 'docker-compose up'
    subprocess.Popen(cmd, shell=True)
    print(cmd)
    print(' -> Creating and launching DB : successful')



    
if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='This script is used to launch ARLIE generation.')
    parser.add_argument("--arlie_metadata_dir", type=str, required=True, help='path to directory containing postgresql.zip file and RiverBasinTiles folder')
    parser.add_argument("--temp_dir", type=str, help='path to temporary directory, current working directory by default')
    parser.add_argument("--docker_file_dir", type=str, help='path to directory containing docker-compose YAML')

    args = parser.parse_args()
    

    launch_db(args.arlie_metadata_dir, args.temp_dir, args.docker_file_dir)

