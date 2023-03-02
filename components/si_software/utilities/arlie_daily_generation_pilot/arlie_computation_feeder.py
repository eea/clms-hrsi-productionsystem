#!/usr/bin/env python3

# Authorizing other packages absolute import
import os, sys, shutil, tempfile
import fileinput
root_folder = '/'.join(os.getcwd().split('cosims')[:-1])
sys.path.append(root_folder+'cosims')

from components.common.python.database.model.job.fsc_rlie_job import FscRlieJob
from components.common.python.database.model.job.rlies1_job import RlieS1Job
from components.common.python.database.model.job.rlies1s2_job import RlieS1S2Job
from components.common.python.database.model.job.job_status import JobStatus
from components.common.python.database.rest.stored_procedure import StoredProcedure
from components.common.python.util.log_util import temp_logger

import boto3
import botocore
import json
import re
import subprocess
import time
import glob
from datetime import datetime, timedelta
import multiprocessing
import pandas as pd
import pyodbc

# Set the default logging level to DEBUG, and output in both console and log file
import logging

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
    URL = 'https://cryo.land.magellium.fr/get_arlie?geometrywkt=%s&cloudcoveragemax=100&startdate=%s&completiondate=%s&getonlysize=True'%(Europe_polygon, day_to_retrieve.strftime('%Y-%m-%d'), tomorrow.strftime('%Y-%m-%d'))
    cmd = ['curl', '-s', URL]
    res = subprocess.check_output(cmd, encoding='UTF-8')
    print("Executing request on DB for day %s"%(day_to_retrieve_str))
    nb_products_start = int(''.join(re.findall("([\d])", res))) 
    
    if nb_products_start > 0:
        print('ARLIE products found int the database, aborting ARLIE computation')
        return
    else:
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


        URL = 'https://cryo.land.magellium.fr/get_arlie?geometrywkt=%s&cloudcoveragemax=100&startdate=%s&completiondate=%s&getonlysize=True'%(Europe_polygon, day_to_retrieve.strftime('%Y-%m-%d'), tomorrow.strftime('%Y-%m-%d'))
        cmd = ['curl', '-s', URL]
        res = subprocess.check_output(cmd, encoding='UTF-8')
        nb_products_end = int(''.join(re.findall("([\d])", res)))
        print("Added %d ARLIE products in the DB"%(nb_products_end - nb_products_start))


    shutil.rmtree(temp_dir)

class ArlieComputationFeeder():
    
    # Set the logger
    logger = logging.getLogger('arlie_computation_feeder')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s][%(name)s line %(lineno)d] -- %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    def __init__(self, config_file):
        
        with open(config_file,'r') as json_config_file:
            arlie_computation_policy = json.loads(json_config_file.read())
            self.LAST_USED_RLIE_T2 = arlie_computation_policy['LAST_USED_RLIE_T2']
            self.LAST_USED_RLIE_TYPE = arlie_computation_policy['LAST_USED_RLIE_TYPE']
            self.LAST_USED_RLIE_JOB_ID = arlie_computation_policy['LAST_USED_RLIE_JOB_ID']
            self.START_MILESTONE = datetime.strptime(arlie_computation_policy['START_MILESTONE'],'%Y-%m-%d')
            self.MAX_THROWBACK = arlie_computation_policy['MAX_THROWBACK']
            self.RLIE_USED_FOR_ARLIE_DIR = arlie_computation_policy['RLIE_USED_FOR_ARLIE_DIR']
            self.RLIE_DIR = arlie_computation_policy['RLIE_DIR']
            self.TEMP_DIR = arlie_computation_policy['TEMP_DIR']
        
        self.start_extraction_date = None
        
        self.rlie_s1_jobs = dict()
        self.rlie_s2_jobs = dict()
        self.rlie_s1s2_jobs = dict()
    
    def set_extraction_date(self, start_date):
        if (self.LAST_USED_RLIE_T2 == 'None') or (self.LAST_USED_RLIE_TYPE == 'None') or (self.LAST_USED_RLIE_JOB_ID == 'None'):
            self.logger.warning(f"Starting the ARLIE computation from start milestone {start_date}")
            self.start_extraction_date = start_date
        else:
            pass

    def extract_rlies_jobs_measured_at_day(self, measurement_date):
        self.rlie_s1_jobs = dict()
        self.rlie_s2_jobs = dict()
        self.rlie_s1s2_jobs = dict()
        if self.LAST_USED_RLIE_T2 == 'None':
            self.logger.info(f'Extract RLIEs jobs which inputs were measured on day {measurement_date} without prior knowledge')
            self.logger.info(f'Extract RLIE S2 jobs measured on day {measurement_date}...')
            rlie_s2_jobs = StoredProcedure.get_jobs_within_measurement_date(FscRlieJob,
                                                                    'measurement_date',
                                                                    measurement_date,
                                                                    measurement_date+timedelta(days=1)-timedelta(microseconds=1),
                                                                    temp_logger.info)
            
            self.logger.info(f'Extracted {len(rlie_s2_jobs)} potentially fertile jobs')
            for job in rlie_s2_jobs:
                if job.rlie_path is not None:
                    self.rlie_s2_jobs[job.rlie_path.split("/")[-1]] = job.rlie_path
                    
            self.logger.info(f'Kept {len(self.rlie_s2_jobs)} fertile jobs')
            self.logger.info(f'[Done]')
            self.logger.info(f'Extract RLIE S1 jobs measured on day {measurement_date}...')
            rlie_s1_jobs = StoredProcedure.get_jobs_within_measurement_date(RlieS1Job,
                                                                    'measurement_date',
                                                                    measurement_date,
                                                                    measurement_date+timedelta(days=1)-timedelta(microseconds=1),
                                                                    temp_logger.info)

            self.logger.info(f'Extracted {len(rlie_s1_jobs)} potentially fertile jobs')
            for job in rlie_s1_jobs:
                if job.rlies1_product_paths_json:
                    for key, value in job.rlies1_product_paths_json.items():
                        self.rlie_s1_jobs[key] = value

            self.logger.info(f'Kept {len(self.rlie_s1_jobs)} fertile jobs')
            self.logger.info(f'[Done]')
            self.logger.info(f'Extract RLIE S1S2 jobs measured on day {measurement_date}...')
            rlie_s1s2_jobs = StoredProcedure.get_jobs_within_measurement_date(RlieS1S2Job,
                                                                    'measurement_date',
                                                                    measurement_date,
                                                                    measurement_date+timedelta(days=1)-timedelta(microseconds=1),
                                                                    temp_logger.info)
            self.logger.info(f'Extracted {len(rlie_s1s2_jobs)} fertile jobs')
            for job in rlie_s1s2_jobs:
                if job.rlies1s2_path is not None:
                    self.rlie_s1s2_jobs[job.rlies1s2_path.split("/")[-1]] = job.rlies1s2_path
                    
            self.logger.info(f'Kept {len(self.rlie_s1s2_jobs)} fertile jobs')
            self.logger.info(f'[Done]')

         
    def download_rlie(self, products_to_download, product_type, product_file_path):
        # Remove file content
        open(product_file_path, 'w').close()

        # Find test/production projects credentials
        access_pattern = r"AWS_ACCESS_KEY_ID=(.*)\n"
        secret_pattern = r"AWS_SECRET_ACCESS_KEY=(.*)\n"
        with open("///home/eouser/prod_env.sh", "r") as file:
            content = file.read()
        prod_env_access_key = re.findall(access_pattern, content)[0].strip('"')
        prod_env_secret_key = re.findall(secret_pattern, content)[0].strip('"')
        
        csi_products_s3 = ArlieComputationFeeder.get_s3('https://cf2.cloudferro.com:8080',prod_env_access_key,prod_env_secret_key)
        csi_products_bucket = 'HRSI'
        
        self.check_bucket(csi_products_s3, csi_products_bucket)
        self.logger.info('Connection to production bucket checked')

        year = str(self.start_extraction_date.year)
        month = "%.2d"%(self.start_extraction_date.month)
        day = "%.2d"%(self.start_extraction_date.day)
        for job in products_to_download:
            if product_type == 'RLIE':
                product_path = job.replace('/eodata/HRSI/', '')
            else:
                product_path = job
                
            product_name = product_path.split('/')[-1]
            

            # Check if the product already exists in the destination bucket
            product_MTD = product_path + "/" + product_name + "_MTD.xml"
            if self.object_exists(csi_products_s3, csi_products_bucket, product_MTD):
                dir_path = os.path.join(self.RLIE_DIR, product_type, year, month, day, product_name)
                
                #download_command = f"s3cmd get s3://HRSI/{product_path}/ {dir_path} --recursive"
                #ArlieComputationFeeder.run_s3cmd(download_command, csi_products_bucket)
                cmd = ['rclone', 'copy', 'eodata:HRSI/%s'%(product_path), dir_path]
                res = subprocess.check_output(cmd, encoding='UTF-8')
                with open(product_file_path, mode='a') as ds:
                    ds.write(product_name + '\n')
            else:

                # Copy the product quicklook from the test project bucket to  
                # the production project bucket.
                self.logger.warning(f"Product '{product_path}' could not be found in production bucket, so we will not download it !")
    
    def compare_and_download_missing_rlie(self, measurement_date):
        measurement_date_str = measurement_date.strftime('%Y-%m-%d')
        files = glob.glob(os.path.join(self.RLIE_USED_FOR_ARLIE_DIR, measurement_date_str + "*"))
        if len(files) == 3:
            rlie_s2_file_path = os.path.join(self.RLIE_USED_FOR_ARLIE_DIR, measurement_date_str + "_RLIE.txt")
            rlie_s1_file_path = os.path.join(self.RLIE_USED_FOR_ARLIE_DIR, measurement_date_str + "_RLIE_S1.txt")
            rlie_s1s2_file_path = os.path.join(self.RLIE_USED_FOR_ARLIE_DIR, measurement_date_str + "_RLIE_S1S2.txt")
            with open(rlie_s2_file_path) as f:
                rlie_s2_list = f.readlines()
                rlie_s2_list = [x.replace('\n', '') for x in rlie_s2_list]
                self.logger.info(f'{len(rlie_s2_list)} RLIE S2 retrieved for ARLIE computation')
                     
            with open(rlie_s1_file_path) as f:
                rlie_s1_list = f.readlines()
                rlie_s1_list = [x.replace('\n', '') for x in rlie_s1_list]
                self.logger.info(f'{len(rlie_s1_list)} RLIE S1 retrieved for ARLIE computation')
                                
            with open(rlie_s1s2_file_path) as f:
                rlie_s1s2_list = f.readlines()
                rlie_s1s2_list = [x.replace('\n', '') for x in rlie_s1s2_list]
                self.logger.info(f'{len(rlie_s1s2_list)} RLIE S1S2 retrieved for ARLIE computation')
                 
                
            rlie_s2_diff = [self.rlie_s2_jobs.get(key) for key in list(set([key for key in self.rlie_s2_jobs]) - set(rlie_s2_list))]
            rlie_s1_diff = [self.rlie_s1_jobs.get(key) for key in list(set([key for key in self.rlie_s1_jobs]) - set(rlie_s1_list))]
            rlie_s1s2_diff = [self.rlie_s1s2_jobs.get(key) for key in list(set([key for key in self.rlie_s1s2_jobs]) - set(rlie_s1s2_list))]

            if len(rlie_s2_list) == 0 and len(rlie_s1_list) == 0 and len(rlie_s1s2_list) == 0:
                self.logger.info(f'No ARLIE were computed for day {measurement_date}, computing ARLIE')
       
                self.download_rlie([self.rlie_s2_jobs.get(key) for key in self.rlie_s2_jobs], 'RLIE', rlie_s2_file_path)
                self.download_rlie([self.rlie_s1_jobs.get(key) for key in self.rlie_s1_jobs], 'RLIE_S1', rlie_s1_file_path)
                self.download_rlie([self.rlie_s1s2_jobs.get(key) for key in self.rlie_s1s2_jobs], 'RLIE_S1S2', rlie_s1s2_file_path)
                make_arlie_update(measurement_date_str, '/home/eouser/arlie_generation/arlie_metadata/', self.RLIE_DIR, temp_dir=self.TEMP_DIR, nprocs=4)
                shutil.rmtree(self.RLIE_DIR)

            elif len(rlie_s2_diff) > 0 or len(rlie_s1_diff) > 0 or len(rlie_s1s2_diff) > 0:
                self.logger.info(f'Some RLIE were missed for day {measurement_date} when computing ARLIE')
                self.logger.info(f'{len(rlie_s2_diff)} RLIE S2')
                self.logger.info(f'{len(rlie_s1_diff)} RLIE S1')
                self.logger.info(f'{len(rlie_s1s2_diff)} RLIE S1S2')
            else:
                self.logger.info(f'Nothing to do for day {measurement_date}')

        else:
            self.logger.info(f'No RLIE files for day {measurement_date}, aborting')       

            

        
    @staticmethod
    def get_s3(endpoint_url: str, access_key: str, secret_key: str):
        '''Build an S3 ressource object'''

        s3_resource = boto3.resource(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url
            )
        return s3_resource


    def check_bucket(self, s3_resource, bucket: str):
        '''Check if a bucket exists and if its available'''

        # We need to access to the S3 client for this resource to reference
        # exceptions and some meta info below.
        s3_client = s3_resource.meta.client
        try:
            s3_client.head_bucket(Bucket=bucket)
        except s3_client.exceptions.ClientError as s3_error:
            code = s3_error.response['Error']['Code']
            if code == '403':
                self.logger.error(
                    f'bad credentials given to connect to the "{bucket}" '
                    f'bucket with the S3 endpoint URL '
                    f'{s3_client.meta.endpoint_url}"'
                )
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise Exception from s3_error
            elif code == '404':
                self.logger.error(
                    f'the bucket "{bucket}" doesn\'t exist in the S3 storage '
                    f'which endpoint URL is "{s3_client.meta.endpoint_url}" and '
                    f'with the given credentials'
                )
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise Exception from s3_error
            elif code == '400':
                self.logger.error(
                    f'failed to do the request to check if bucket "{bucket}" '
                    f'exists, with endpoint URL "{s3_client.meta.endpoint_url}" '
                    f'and with the given credentials. The HTTP error code is '
                    f'{code}. This can happen when credentials are given but set '
                    f'to empty strings (for both access and secret keys).'
                )
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise Exception from s3_error
            else:
                self.logger.error(
                    f'failed to do the request to check if bucket "{bucket}" '
                    f'exists, with endpoint URL "{s3_client.meta.endpoint_url}" '
                    f'and with the given credentials. The HTTP error code is '
                    f'{code}. Could not tell what is going one.'
                )
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise Exception from s3_error
        except botocore.exceptions.EndpointConnectionError as s3_error:
            self.logger.error(
                f'could not connect to the S3 endpoint URL '
                f'"{s3_client.meta.endpoint_url}", either there is an error in the '
                f'URL or the endpoint is not responding'
            )
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise Exception from s3_error
        except Exception as error:
            self.logger.error(
                f'an unexpected error occured during the check of the bucket '
                f'{bucket}, raise an external error so that it might be tried '
                f'again in case it is due to a temporary issue'
            )
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise Exception from error

    def object_exists(self, s3_resource, bucket, object_in_bucket):
        '''Check if an object exists in a bucket'''

        s3_client = s3_resource.meta.client
        try:
            # This command will fail if object doesn't exist on the S3 bucket.
            s3_resource.Object(bucket, object_in_bucket).load()
            object_exists = True
        except s3_client.exceptions.ClientError as s3_error:
            code = s3_error.response['Error']['Code']
            if code == '404':
                object_exists = False
            else:
                logging.error(
                    f'an unexpected error occured during the check of object '
                    f'{object_in_bucket} existence in the bucket {bucket}, raise an '
                    f'external error so that it might be tried again in case it is due '
                    f'to a temporary issue'
                )
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise Exception from s3_error

        except Exception as error:
            logging.error(
                f'an unexpected error occured during the check of object '
                f'{object_in_bucket} existence in the bucket {bucket}, raise an '
                f'external error so that it might be tried again in case it is due '
                f'to a temporary issue'
            )
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise Exception from error
        return object_exists

    @staticmethod
    def get_prefixed_objects(s3_resource, bucket: str, objects_prefix: str, objects_suffix: str=''):
        '''Get all objects matching a prefix pattern from a bucket'''

        s3_client = s3_resource.meta.client

        s3_keys = []

        next_token = ''
        base_kwargs = {
            'Bucket': bucket,
            'Prefix': objects_prefix,
        }

        while next_token is not None:
            kwargs = base_kwargs.copy()
            if next_token != '':
                kwargs.update({'ContinuationToken': next_token})

            try:
                results = s3_client.list_objects_v2(**kwargs)
            except Exception as error:
                logging.error(
                    f'an unexpected error occured during getting the list of '
                    f'objects with prefix {objects_prefix} in the bucket {bucket}, '
                    f'raise an external error so that it might be tried again in '
                    f'case it is due to a temporary issue'
                )
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise Exception from error

            for s3_object in results['Contents']:
                if (
                    len(objects_suffix) == 0
                    or (len(objects_suffix) > 0 and s3_object['Key'].endswith(objects_suffix))
                ):
                    s3_keys.append(s3_object['Key'])

            next_token = results.get('NextContinuationToken')

        return s3_keys

    @staticmethod
    def download_file_from_bucket(
        s3_resource, bucket: str, object_in_bucket: str, local_file_name: str
    ):
        # We need to access to the S3 client for this resource to reference
        # exceptions and some meta info below.
        s3_client = s3_resource.meta.client
        ArlieComputationFeeder.check_bucket(s3_resource, bucket)
        try:
            # This command will fail if object doesn't exist on the S3 bucket.
            s3_resource.Object(bucket, object_in_bucket).load()
        except s3_client.exceptions.ClientError as s3_error:
            code = s3_error.response['Error']['Code']
            if code == '404':
                logging.error(
                    f'the object "{object_in_bucket}" doesn\'t exist in the bucket '
                    f'"{bucket}" in the S3 storage which endpoint URL is '
                    f'"{s3_client.meta.endpoint_url}" and with the given '
                    f'credentials'
                )
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise Exception from s3_error
            else:
                logging.error(
                    f'an unknown error occured while downloading the object '
                    f'"{object_in_bucket}" from the bucket "{bucket}" in the S3 '
                    f'storage which endpoint URL is '
                    f'"{s3_client.meta.endpoint_url}"; HTTP status = {code}'
                )
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise Exception from s3_error

        # Everything is fine on the S3 side, we can launch the download
        try:
            s3_resource.Bucket(bucket).download_file(
                object_in_bucket, local_file_name
                )
        except PermissionError as error:
            logging.error(
                f'could not write the downloaded file from S3 storage due to '
                f'permission error for "{local_file_name}"'
            )
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise Exception from error
        except IsADirectoryError as error:
            logging.error(
                f'could not write the downloaded file from S3 storage because '
                f'the target is a directory "{local_file_name}"'
            )
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise Exception from error
        except Exception as error:
            logging.error(
                f'an unexpected error occured during the download of '
                f'{object_in_bucket} from the bucket {bucket}, raise an '
                f'external error so that it might be tried again in case '
                f'it is due to a temporary issue'
            )
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise Exception from error

    @staticmethod
    def run_s3cmd(command: str, bucket: str):
        '''Send and s3cmd command on a specific bucket, and ensure that it succeeded.'''

        # Set the command prefix, which provide access to the desired bucket
        if bucket == "HRSI":
            command_prefix = ". /home/eouser/prod_env.sh; "
        elif (bucket == "reprocessing" or bucket == "reprocessing-published"):
            command_prefix = ". test_env.sh; "
        else:
            logging.error(f"Unrecognized bucket, '{bucket}', to run s3cmd : '{command}' !")
            raise Exception

        process = subprocess.Popen(command_prefix + command, shell=True, 
            executable="/bin/bash", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        # If the command fail we try to run it again after a 1 minute delay
        if process.returncode != 0:
            logging.warning(f"S3cmd '{command_prefix + command}' failed ! We will "\
                f"try to run it again in a minute, stderr : \n    {stderr}")
            time.sleep(60)

            ret = ArlieComputationFeeder.run_s3cmd(command, bucket)
            return ret

        return stdout.decode('ascii')


        
if __name__ == "__main__":
    config_file = os.path.dirname(os.path.realpath(__file__))+"/config_arlie_feeder.json"
    arlie_computation_feeder_instance = ArlieComputationFeeder(config_file)
    start_date = arlie_computation_feeder_instance.START_MILESTONE
    arlie_computation_feeder_instance.set_extraction_date(start_date)
    arlie_computation_feeder_instance.extract_rlies_jobs_measured_at_day(arlie_computation_feeder_instance.start_extraction_date)
    arlie_computation_feeder_instance.compare_and_download_missing_rlie(arlie_computation_feeder_instance.start_extraction_date)

    with fileinput.FileInput(config_file, inplace=True, backup='.bak') as file:
        for line in file:
            print(line.replace(start_date.strftime('%Y-%m-%d'), (start_date + timedelta(days=1)).strftime('%Y-%m-%d')), end='')  