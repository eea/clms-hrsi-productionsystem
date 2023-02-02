#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, shutil, subprocess, tempfile
from datetime import datetime, timedelta
import glob
import multiprocessing
import json
import re

compute_arlie_docker_image_default = 'si_software_part2:latest'
export_arlie_docker_image_default = 'si_arlie_export:2_21'

port_number_base = 5432
Europe_polygon = 'POLYGON((-8.521263215755956+74.27442378824767%2C-41.81467638940865+46.29752415308005%2C12.556581095621294+26.022285227748796%2C72.67634157466321+30.863991364289717%2C60.46077271238776+74.78548595583968%2C-8.521263215755956+74.27442378824767))'


def make_appsettings_json(output_file, ip_number, port_number):
    dico = {
    "Configuration" : {
        "SnapPath": "/install/snap/bin/gpt",
        "GdalFgdbPath": "/install/gdal-2.3.1/apps",
        "TileCodes": "/work/part2/ice_s1/TileCodes/TileCodes.txt",
        "GraphPath": "/install/ice/graphs/preprocessing_classification_fre_5class_md20m_1input_14flat_bsi_cut_auto.xml",
        "GraphS1Path": "/install/ice/graphs/grd_preprocessing_threshold_speckle_filtering_Sim_subs_rep_2_auto.xml",
        "RlieMetadataTemplatePath": "/install/ice/metadata/RLIE_Metadata.xml",
        "RlieS1MetadataTemplatePath": "/install/ice/metadata/RLIE_S1_Metadata.xml",
        "RlieS1S2MetadataTemplatePath": "/install/ice/metadata/RLIE_S1S2_Metadata.xml",
        "ArlieMetadataTemplatePath": "/install/ice/metadata/ARLIE_Metadata.xml",
        "ArlieS1S2MetadataTemplatePath": "/install/ice/metadata/ARLIE_S1S2_Metadata.xml",
        "PostgreHost": "%s"%ip_number,
        "PostgrePort": '%d'%port_number,
        "PostgreDb": "cosims",
        "PostgreUser": "postgres",
        "PostgrePassword": "cosims",
        "MaxThreads": '1',
        "ProductVersion": "V100",
        "GenerationMode": '1',
        "HelpDeskEmail": "helpdesk@email.com",
        "PumUrl": "https://pum.url",
        "DiasUrl": "https://dias.url",
        "DiasPortalName": "Dias Portal Name",
        "ValidationReportFilename": "hrsi-ice-qar",
        "ValidationReportDate": "2021-07-01"
    },
    "Serilog": {
        "MinimumLevel": "Debug",
        "WriteTo": [
            { "Name": "Console",
                "Args": { "restrictedToMinimumLevel": "Information" } 
            },
            { "Name": "Async",
                "Args":  {
                "configure": [
                    { "Name": "File", 
                        "Args": { "path": "/work/si/Logs/ProcessRiverIce.log" },
                        "outputTemplate": "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level}] [{SourceContext}] [{EventId}] {Message}{NewLine}{Exception}",
                        "buffered": 'true'}
                ]
            }
        }]   
    }
}
    with open(output_file, mode='w') as ds:
        json.dump(dico, ds, indent=4)





def rezipfile(zipfile_in, zipfile_out=None, permissions='rx', temp_dir=None):
    if temp_dir is None:
        temp_dir = os.getcwd()
    temp_dir = os.path.abspath(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)
    temp_dir_session = None
    try:
        temp_dir_session = tempfile.mkdtemp(dir=temp_dir, prefix='rezip')
        cmd = 'cd %s; unzip %s; chmod -R u+%s %s; zip -r %s %s'%(temp_dir_session, os.path.abspath(zipfile_in), permissions, os.path.basename(zipfile_in).replace('.zip', ''), \
            os.path.basename(zipfile_in), os.path.basename(zipfile_in).replace('.zip', ''))
        print(cmd)
        subprocess.check_call(cmd, shell=True)
        zipfile_new_path = os.path.join(temp_dir_session, os.path.basename(zipfile_in))
        assert os.path.exists(zipfile_new_path)
        if zipfile_out is not None:
            shutil.move(zipfile_new_path, zipfile_out)
        else:
            shutil.move(zipfile_new_path, zipfile_in)
    finally:
        if temp_dir_session is not None:
            shutil.rmtree(temp_dir_session)


def make_arlie_for_basin(basin_loc, arlie_metadata_dir, rlie_data_dir, start_date, end_date, compute_arlie_docker_image, export_arlie_docker_image, temp_dir, interactive_str, port_number):
    
    temp_dir = os.path.abspath(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)
    
    cwd = os.path.abspath(os.getcwd())

    start_time1 = datetime.utcnow()
    print('Processing basin %s'%basin_loc)
    
    temp_dir_session = None
    postgresql_docker_name = 'arlie_postgres'
    try:
        #create temp dir
        temp_dir_session = tempfile.mkdtemp(dir=temp_dir, prefix='arlie_%s'%basin_loc)
        os.chdir(cwd)

        #creating appsettings_arlies1s2.json
        #get ip of docker container postgresql_docker_name
        ip_number_loc = subprocess.check_output("docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' %s"%postgresql_docker_name, shell=True).decode('utf-8').replace('\n','')
        make_appsettings_json(os.path.join(temp_dir_session, 'appsettings_arlies1s2.json'), ip_number_loc, port_number)
        
        #launch ARLIE creation
        print('Launching ARLIE creation...')
        temp_dir_arlie_gen = os.path.join(temp_dir_session, 'arlie_gen')
        os.makedirs(temp_dir_arlie_gen)
        cmd = 'docker run --rm%s --network=arlie_daily_generation_pilot_postgres -v %s:/arlie_metadata_dir -v %s:/rlie_data_dir -v %s:/temp_dir -v %s:/temp_dir_base %s ProcessRiverIce ARLIES1S2 '%(interactive_str, \
            os.path.abspath(arlie_metadata_dir), os.path.abspath(rlie_data_dir), temp_dir_arlie_gen, temp_dir_session, compute_arlie_docker_image)
        cmd += '%s %s /rlie_data_dir/RLIE /rlie_data_dir/RLIE_S1 /rlie_data_dir/RLIE_S1S2 '%(start_date, end_date)
        cmd += '{0} /arlie_metadata_dir/RiverBasinTiles/{0}.txt /temp_dir /temp_dir_base/appsettings_arlies1s2.json'.format(basin_loc)
        print(cmd)
        subprocess.check_call(cmd, shell=True)
        print(' -> Launching ARLIE creation : successful')

        return {'basin': basin_loc}

    finally:
        print("TODO")
        
               

def make_arlie_update(day_to_retrieve_str, arlie_metadata_dir, rlie_data_dir, compute_arlie_docker_image=None, export_arlie_docker_image=None, temp_dir=None, interactive=False, nprocs=None):
        
    if compute_arlie_docker_image is None:
        compute_arlie_docker_image = compute_arlie_docker_image_default
    if export_arlie_docker_image is None:
        export_arlie_docker_image = export_arlie_docker_image_default
    
    
    if interactive:
        interactive_str = ' -it'
    else:
        interactive_str = ''
    
    if nprocs is None:
        nprocs = 1
    

    temp_dir = os.path.abspath(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)

    day_to_retrieve = datetime.strptime(day_to_retrieve_str, "%Y-%m-%d")
    tomorrow = day_to_retrieve + timedelta(1)
    URL = 'https://cryo.land.copernicus.eu/arlie/get_arlie?geometrywkt=%s&cloudcoveragemax=100&startdate=%s&completiondate=%s&getonlysize=True'%(Europe_polygon, day_to_retrieve.strftime('%Y-%m-%d'), tomorrow.strftime('%Y-%m-%d'))
    cmd = ['curl', '-s', URL]
    res = subprocess.check_output(cmd, encoding='UTF-8')
    print("Executing request on DB for day %s"%(day_to_retrieve_str))
    nb_products = int(''.join(re.findall("([\d])", res)))
    if nb_products > 5:
        print('ARLIE products found for day %s, aborting update'%(day_to_retrieve_str))
        exit()

    products_type = ['RLIE', 'RLIE_S1', 'RLIE_S1S2']
    year = str(day_to_retrieve.year)
    month = "%.2d"%(day_to_retrieve.month)
    day = "%.2d"%(day_to_retrieve.day)
    print("Requesting RLIE")
    for product_type in products_type:
        os.makedirs(os.path.join(rlie_data_dir, product_type, year, month, day))
        path = "eodata:HRSI/CLMS/Pan-European/High_Resolution_Layers/Ice/%s/%s/%s/%s"%(product_type, year, month, day)
        cmd = ['rclone', 'copy', path, os.path.join(rlie_data_dir, product_type, year, month, day)]
        res = subprocess.check_output(cmd, encoding='UTF-8')
        
        with open('/home/eouser/arlie_generation/daily_rlie_retrieval/%s_%s.txt'%(day_to_retrieve_str, product_type), 'a') as the_file:
            for file in glob.glob(os.path.join(rlie_data_dir, product_type, year, month, day) + "/*/"):
                the_file.write(file.split('/')[-2] + '\n')
    
    
    basin_list = sorted([el.replace('.txt', '') for el in os.listdir(os.path.join(arlie_metadata_dir, 'RiverBasinTiles'))])
    if nprocs == 1:
        for basin_loc in basin_list:
            dico_loc = make_arlie_for_basin(basin_loc, arlie_metadata_dir, rlie_data_dir, day_to_retrieve_str, day_to_retrieve_str, compute_arlie_docker_image, export_arlie_docker_image, \
                os.path.join(temp_dir, basin_loc), interactive_str, port_number_base)
            print('%s processed'%dico_loc['basin'])
    else:
        pool = multiprocessing.Pool(nprocs)
        for dico_loc in pool.starmap(make_arlie_for_basin, [[basin_loc, arlie_metadata_dir, rlie_data_dir, day_to_retrieve_str, day_to_retrieve_str, compute_arlie_docker_image, export_arlie_docker_image, \
            os.path.join(temp_dir, basin_loc), interactive_str, port_number_base] for ii_basin, basin_loc in enumerate(basin_list)]):
            shutil.rmtree(os.path.join(temp_dir, dico_loc['basin']))
            print('%s processed'%dico_loc['basin'])


    URL = 'https://cryo.land.copernicus.eu/arlie/get_arlie?geometrywkt=%s&cloudcoveragemax=100&startdate=%s&completiondate=%s&getonlysize=True'%(Europe_polygon, day_to_retrieve.strftime('%Y-%m-%d'), tomorrow.strftime('%Y-%m-%d'))
    cmd = ['curl', '-s', URL]
    res = subprocess.check_output(cmd, encoding='UTF-8')
    nb_products = int(''.join(re.findall("([\d])", res)))
    print("Found %d ARLIE products"%(nb_products))
    if nb_products < 100000:
        print('WARNING: few ARLIE products found for day %s (only %d)'%(day_to_retrieve_str, nb_products))

    shutil.rmtree(temp_dir)
    shutil.rmtree(rlie_data_dir)
            

        
    
if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='This script is used to launch ARLIE generation.')
    parser.add_argument("--arlie_metadata_dir", type=str, required=True, help='path to directory containing postgresql.zip file and RiverBasinTiles folder')
    parser.add_argument("--rlie_data_dir", type=str, required=True, help='path to directory containing RLIE_S2, RLIE_S1 and RLIE_S1S2 directories with at least the products for the period requested')
    parser.add_argument("--day_to_retrieve", type=str, required=True, help='day_to_retrieve in %Y-%m-%d format')
    parser.add_argument("--compute_arlie_docker_image", type=str, help='compute_arlie_docker_image, %s by default'%compute_arlie_docker_image_default, default=compute_arlie_docker_image_default)
    parser.add_argument("--export_arlie_docker_image", type=str, help='export_arlie_docker_image, %s by default'%export_arlie_docker_image_default, default=export_arlie_docker_image_default)
    parser.add_argument("--temp_dir", type=str, help='path to temporary directory, current working directory by default')
    parser.add_argument("--interactive", action='store_true', help='make docker processes interactive')
    parser.add_argument("--nprocs", type=int, default=1, help='number of processes to use')
    args = parser.parse_args()
    

    make_arlie_update(args.day_to_retrieve, args.arlie_metadata_dir, args.rlie_data_dir, \
        compute_arlie_docker_image=args.compute_arlie_docker_image, export_arlie_docker_image=args.export_arlie_docker_image, \
        temp_dir=args.temp_dir, interactive=args.interactive, nprocs=args.nprocs)

