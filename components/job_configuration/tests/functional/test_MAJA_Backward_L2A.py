import logging
from time import sleep
import pytest

from ....common.python.util.datetime_util import DatetimeUtil

# check if environment variable is set, exit in error if it's not
from ....common.python.util.sys_util import SysUtil
SysUtil.ensure_env_var_set("COSIMS_DB_HTTP_API_BASE_URL")
SysUtil.ensure_env_var_set("CSI_SIP_DATA_BUCKET")
SysUtil.ensure_env_var_set("CSI_SCIHUB_ACCOUNT_PASSWORD")

from ....common.python.sentinel.sentinel2_product import Sentinel2Product
from ....common.python.database.model.job.fsc_rlie_job import FscRlieJob
from ....common.python.database.model.job.job_status import JobStatus
from ....common.python.database.rest.stored_procedure import StoredProcedure
from ....common.python.util.log_util import temp_logger

@pytest.fixture
def empty_database():
    StoredProcedure.empty_database(temp_logger.debug)
    
def test_MAJA_backward_L2A_behaviour(empty_database):
    """ Test that checks for the behaviour in case of chronological lack of respect in MAJA L2A computing"""
    
    ### Setting up a job to be finished with a L2A generated at Day 0
    # Extracting valid data
    product_identifier_final =  "/eodata/Sentinel-2/MSI/L1C/2022/08/27/S2A_MSIL1C_"\
                                "20220827T130311_N0400_R038_T28WEU_20220827T182117.SAFE"
    published_final = DatetimeUtil.fromRfc3339("2022-08-28T21:34:14.140863Z")
    cloud_cover_final = 2.890630875175
    snow_cover_final = 0
    s2_product_final = Sentinel2Product(product_identifier_final, published_final, cloud_cover_final, snow_cover_final)
    
    # Create the base job to store in DataBase
    job_first = FscRlieJob(tile_id=s2_product_final.tile_id,
                    l1c_id=s2_product_final.product_id,
                    l1c_path=s2_product_final.product_path,
                    l1c_cloud_cover=s2_product_final.cloud_cover,
                    l1c_snow_cover=s2_product_final.snow_cover,
                    measurement_date=s2_product_final.measurement_date,
                    l1c_esa_creation_date=s2_product_final.esa_creation_date,
                    # There is also the 'update' param that we don't use.
                    l1c_dias_publication_date=s2_product_final.dias_publication_date)
    
    #Setting up the job and posting it on the DB
    job_first.nrt = True
    job_first.post(post_foreign=True)
    
    # Putting it in a step from which we can control it completely
    job_first.post_new_status_change(JobStatus.initialized)
    sleep(0.5)
    job_first.post_new_status_change(JobStatus.internal_error)
    sleep(0.5)
    job_first.post_new_status_change(JobStatus.error_checked)
    sleep(0.5)
    
    # Parametrizing the job as it it was finished
    job_first.l1c_id = "S2A_MSIL1C_20220827T130311_N0400_R038_T28WEU_20220827T182117"
    job_first.l1c_id_list = None
    job_first.l1c_reference_job = True
    job_first.l1c_cloud_cover = 2.890630875175
    job_first.l1c_snow_cover = 0
    job_first.l1c_path = "/eodata/Sentinel-2/MSI/L1C/2022/08/27/S2A_MSIL1C_20220827T130311_N0400_R038_T28WEU_20220827T182117.SAFE"
    job_first.l1c_path_list = None
    job_first.l2a_path_in = "/hidden_value/28WEU/L2A/reference/S2B_MSIL1C_20220822T130259_N0400_R038_T28WEU_20220822T151421/nominal"
    job_first.l2a_path_out = "/hidden_value/28WEU/L2A/reference/S2A_MSIL1C_20220827T130311_N0400_R038_T28WEU_20220827T182117/nominal"
    job_first.save_full_l2a = False
    job_first.l2a_status = "generated"
    job_first.n_jobs_run_since_last_init = 65
    job_first.n_l2a_produced_since_last_init = 66
    job_first.dtm_path = None
    job_first.fsc_infos = '{"collection_name": "HR-S&I", "resto": {"type": "Feature", "geometry": {"wkt": "POLYGON ((-15.000453459106 66.08681580800796, -15.000453459106 66.29749731480275, -15.000453459106 66.50817882159755, -15.000453459106 66.71886032839235, -14.97772525847875 66.71886032839235, -14.95499705785149 66.71886032839235, -14.93226885722424 66.71886032839235, -14.78431272765073 66.68521367844416, -14.63635659807723 66.65156702849598, -14.48840046850373 66.61792037854778, -14.46322118741667 66.61212691564279, -14.43804190632961 66.60633345273781, -14.41286262524256 66.60053998983282, -14.40383819264056 66.59831173486937, -14.39481376003856 66.5960834799059, -14.38578932743656 66.59385522494244, -14.3840067234658 66.59318674845341, -14.38222411949503 66.59251827196438, -14.38044151552427 66.59184979547534, -14.38044151552427 66.59140414448265, -14.38044151552427 66.59095849348995, -14.38044151552427 66.59051284249726, -14.38055292827244 66.5895101277637, -14.38066434102061 66.58850741303016, -14.38077575376878 66.5875046982966, -14.39180561583789 66.57814602745009, -14.402835477907 66.56878735660356, -14.41386533997611 66.55942868575706, -14.41564794394688 66.55798032003081, -14.41743054791764 66.55653195430456, -14.41921315188841 66.55508358857831, -14.61206861897556 66.3989943283882, -14.80492408606271 66.24290506819807, -14.99777955314986 66.08681580800796, -14.99867085513524 66.08681580800796, -14.99956215712062 66.08681580800796, -15.000453459106 66.08681580800796))"}, "properties": {"productIdentifier": "/eodata/HRSI/CLMS/Pan-European/High_Resolution_Layers/Snow/FSC/2022/08/27/FSC_20220827T130318_S2A_T28WEU_V102_1", "title": "FSC_20220827T130318_S2A_T28WEU_V102_1", "resourceSize": 1286144, "organisationName": "EEA", "startDate": "2022-08-27T13:03:18.613000", "completionDate": "2022-08-29T00:57:19.162831", "productType": "FSC", "resolution": 20, "mission": "S2", "cloudCover": "18.0", "processingBaseline": "V102", "host_base": "s3.waw2-1.cloudferro.com", "s3_bucket": "HRSI", "thumbnail": "Preview/CLMS/Pan-European/High_Resolution_Layers/Snow/FSC/2022/08/27/FSC_20220827T130318_S2A_T28WEU_V102_1/thumbnail.png"}}}'
    job_first.rlie_infos = '{"collection_name": "HR-S&I", "resto": {"type": "Feature", "geometry": {"wkt": "POLYGON ((-15.000453459106 66.08681580800796, -15.000453459106 66.29749731480275, -15.000453459106 66.50817882159755, -15.000453459106 66.71886032839235, -14.97772525847875 66.71886032839235, -14.95499705785149 66.71886032839235, -14.93226885722424 66.71886032839235, -14.78431272765073 66.68521367844416, -14.63635659807723 66.65156702849598, -14.48840046850373 66.61792037854778, -14.46322118741667 66.61212691564279, -14.43804190632961 66.60633345273781, -14.41286262524256 66.60053998983282, -14.40383819264056 66.59831173486937, -14.39481376003856 66.5960834799059, -14.38578932743656 66.59385522494244, -14.3840067234658 66.59318674845341, -14.38222411949503 66.59251827196438, -14.38044151552427 66.59184979547534, -14.38044151552427 66.59140414448265, -14.38044151552427 66.59095849348995, -14.38044151552427 66.59051284249726, -14.38055292827244 66.5895101277637, -14.38066434102061 66.58850741303016, -14.38077575376878 66.5875046982966, -14.39180561583789 66.57814602745009, -14.402835477907 66.56878735660356, -14.41386533997611 66.55942868575706, -14.41564794394688 66.55798032003081, -14.41743054791764 66.55653195430456, -14.41921315188841 66.55508358857831, -14.61206861897556 66.3989943283882, -14.80492408606271 66.24290506819807, -14.99777955314986 66.08681580800796, -14.99867085513524 66.08681580800796, -14.99956215712062 66.08681580800796, -15.000453459106 66.08681580800796))"}, "properties": {"productIdentifier": "/eodata/HRSI/CLMS/Pan-European/High_Resolution_Layers/Ice/RLIE/2022/08/27/RLIE_20220827T130318_S2A_T28WEU_V102_1", "title": "RLIE_20220827T130318_S2A_T28WEU_V102_1", "resourceSize": 356352, "organisationName": "EEA", "startDate": "2022-08-27T13:03:18.613000", "completionDate": "2022-08-29T00:57:19.162831", "productType": "RLIE", "resolution": 20, "mission": "S2", "cloudCover": "18.0", "processingBaseline": "V102", "host_base": "s3.waw2-1.cloudferro.com", "s3_bucket": "HRSI", "thumbnail": "Preview/CLMS/Pan-European/High_Resolution_Layers/Ice/RLIE/2022/08/27/RLIE_20220827T130318_S2A_T28WEU_V102_1/thumbnail.png"}}}'
    job_first.fsc_path = "/eodata/HRSI/CLMS/Pan-European/High_Resolution_Layers/Snow/FSC/2022/08/27/FSC_20220827T130318_S2A_T28WEU_V102_1"
    job_first.rlie_path = "/eodata/HRSI/CLMS/Pan-European/High_Resolution_Layers/Ice/RLIE/2022/08/27/RLIE_20220827T130318_S2A_T28WEU_V102_1"
    job_first.measurement_date = "2022-08-27T13:03:11"
    job_first.l1c_sensing_time = "2022-08-27T13:03:18.613"
    job_first.l1c_esa_creation_date = "2022-08-27T18:21:17"
    job_first.l1c_esa_publication_date = "2022-08-27T19:37:58.208"
    job_first.l1c_dias_publication_date = "2022-08-28T21:34:14.140863"
    job_first.fsc_completion_date = "2022-08-29T00:57:19.162831"
    job_first.rlie_completion_date = "2022-08-29T00:57:19.162831"
    job_first.fsc_json_publication_date = "2022-08-29T01:02:30.671643"
    job_first.rlie_json_publication_date = "2022-08-29T01:02:30.699326"
    job_first.maja_mode = "nominal"
    job_first.maja_threads = 1
    job_first.maja_other_params = None
    job_first.maja_return_code = 0
    job_first.backward_reprocessing_run = None
    job_first.reprocessing_context = "None"
    job_first.name = "28WEU-2022-08-27"
    job_first.priority = "delayed"
    job_first.nomad_id = "si-processing/dispatch-1661727647-0469afc7"
    job_first.tile_id = "28WEU"
    job_first.next_log_level = 20
    job_first.next_log_file_path = None
    job_first.print_to_orch = None
    
    # Applying the changes on the job on the DB
    job_first.patch(patch_foreign=True)
    sleep(1)
    
    # Setting the job at done
    job_first.post_new_status_change(JobStatus.done)


    ### Setting up a job to be finished with a L2A generated at Day 2 > Day 0
    # Extracting valid data
    product_identifier_final =  "/eodata/Sentinel-2/MSI/L1C/2022/09/06/S2A_MSIL1C_"\
                              "20220906T130311_N0400_R038_T28WEU_20220906T182127.SAFE"
    published_final = DatetimeUtil.fromRfc3339("2022-09-06T20:12:38.655863Z")
    cloud_cover_final = 0
    snow_cover_final = 0
    s2_product_final = Sentinel2Product(product_identifier_final, published_final, cloud_cover_final, snow_cover_final)
    
    # Create the base job to store in DataBase
    job_final = FscRlieJob(tile_id=s2_product_final.tile_id,
                    l1c_id=s2_product_final.product_id,
                    l1c_path=s2_product_final.product_path,
                    l1c_cloud_cover=s2_product_final.cloud_cover,
                    l1c_snow_cover=s2_product_final.snow_cover,
                    measurement_date=s2_product_final.measurement_date,
                    l1c_esa_creation_date=s2_product_final.esa_creation_date,
                    # There is also the 'update' param that we don't use.
                    l1c_dias_publication_date=s2_product_final.dias_publication_date)
    
    #Setting up the job and posting it on the DB
    job_final.nrt = True
    job_final.post(post_foreign=True)
    
    # Putting it in a step from which we can control it completely
    job_final.post_new_status_change(JobStatus.initialized)
    sleep(0.5)
    job_final.post_new_status_change(JobStatus.internal_error)
    sleep(0.5)
    job_final.post_new_status_change(JobStatus.error_checked)
    sleep(0.5)
    
    # Parametrizing the job as it it was finished
    job_final.l1c_id = "S2A_MSIL1C_20220906T130311_N0400_R038_T28WEU_20220906T182127"
    job_final.l1c_id_list = None
    job_final.l1c_reference_job = True
    job_final.l1c_cloud_cover = 0
    job_final.l1c_snow_cover = 0
    job_final.l1c_path = "/eodata/Sentinel-2/MSI/L1C/2022/09/06/S2A_MSIL1C_20220906T130311_N0400_R038_T28WEU_20220906T182127.SAFE"
    job_final.l1c_path_list = None
    job_final.l2a_path_in = None
    job_final.l2a_path_out = "/hidden_value/28WEU/L2A/reference/S2A_MSIL1C_20220906T130311_N0400_R038_T28WEU_20220906T182127/init"
    job_final.save_full_l2a = False
    job_final.job_id_for_last_valid_l2a = 1
    job_final.l2a_status = "generated"
    job_final.n_jobs_run_since_last_init = 0
    job_final.n_l2a_produced_since_last_init = 1
    job_final.dtm_path = None
    job_final.fsc_infos = '{"collection_name": "HR-S&I", "resto": {"type": "Feature", "geometry": {"wkt": "POLYGON ((-15.000453459106 66.08915547571958, -15.000453459106 66.29905709327717, -15.000453459106 66.50895871083476, -15.000453459106 66.71886032839235, -14.97961927519769 66.71886032839235, -14.95878509128937 66.71886032839235, -14.93795090738105 66.71886032839235, -14.77729372451585 66.68231694699166, -14.61663654165064 66.64577356559099, -14.45597935878544 66.6092301841903, -14.44338971824191 66.6063334527378, -14.43080007769838 66.60343672128532, -14.41821043715485 66.60053998983282, -14.40562079661132 66.59764325838033, -14.39303115606779 66.59474652692784, -14.38044151552427 66.59184979547534, -14.38044151552427 66.59140414448265, -14.38044151552427 66.59095849348995, -14.38044151552427 66.59051284249726, -14.38055292827244 66.5895101277637, -14.38066434102061 66.58850741303016, -14.38077575376878 66.5875046982966, -14.38255835773955 66.58605633257035, -14.38434096171031 66.5846079668441, -14.38612356568108 66.58315960111786, -14.59000889483734 66.41849155931843, -14.7938942239936 66.25382351751901, -14.99777955314986 66.08915547571958, -14.99867085513524 66.08915547571958, -14.99956215712062 66.08915547571958, -15.000453459106 66.08915547571958))"}, "properties": {"productIdentifier": "/eodata/HRSI/CLMS/Pan-European/High_Resolution_Layers/Snow/FSC/2022/09/06/FSC_20220906T130317_S2A_T28WEU_V102_0", "title": "FSC_20220906T130317_S2A_T28WEU_V102_0", "resourceSize": 1032192, "organisationName": "EEA", "startDate": "2022-09-06T13:03:17.590000", "completionDate": "2022-09-13T12:53:16.246992", "productType": "FSC", "resolution": 20, "mission": "S2", "cloudCover": "0.0", "processingBaseline": "V102", "host_base": "s3.waw2-1.cloudferro.com", "s3_bucket": "HRSI", "thumbnail": "Preview/CLMS/Pan-European/High_Resolution_Layers/Snow/FSC/2022/09/06/FSC_20220906T130317_S2A_T28WEU_V102_0/thumbnail.png"}}}'
    job_final.rlie_infos = '{"collection_name": "HR-S&I", "resto": {"type": "Feature", "geometry": {"wkt": "POLYGON ((-15.000453459106 66.08915547571958, -15.000453459106 66.29905709327717, -15.000453459106 66.50895871083476, -15.000453459106 66.71886032839235, -14.97961927519769 66.71886032839235, -14.95878509128937 66.71886032839235, -14.93795090738105 66.71886032839235, -14.77729372451585 66.68231694699166, -14.61663654165064 66.64577356559099, -14.45597935878544 66.6092301841903, -14.44338971824191 66.6063334527378, -14.43080007769838 66.60343672128532, -14.41821043715485 66.60053998983282, -14.40562079661132 66.59764325838033, -14.39303115606779 66.59474652692784, -14.38044151552427 66.59184979547534, -14.38044151552427 66.59140414448265, -14.38044151552427 66.59095849348995, -14.38044151552427 66.59051284249726, -14.38055292827244 66.5895101277637, -14.38066434102061 66.58850741303016, -14.38077575376878 66.5875046982966, -14.38255835773955 66.58605633257035, -14.38434096171031 66.5846079668441, -14.38612356568108 66.58315960111786, -14.59000889483734 66.41849155931843, -14.7938942239936 66.25382351751901, -14.99777955314986 66.08915547571958, -14.99867085513524 66.08915547571958, -14.99956215712062 66.08915547571958, -15.000453459106 66.08915547571958))"}, "properties": {"productIdentifier": "/eodata/HRSI/CLMS/Pan-European/High_Resolution_Layers/Ice/RLIE/2022/09/06/RLIE_20220906T130317_S2A_T28WEU_V102_0", "title": "RLIE_20220906T130317_S2A_T28WEU_V102_0", "resourceSize": 356352, "organisationName": "EEA", "startDate": "2022-09-06T13:03:17.590000", "completionDate": "2022-09-13T12:53:16.246992", "productType": "RLIE", "resolution": 20, "mission": "S2", "cloudCover": "0.0", "processingBaseline": "V102", "host_base": "s3.waw2-1.cloudferro.com", "s3_bucket": "HRSI", "thumbnail": "Preview/CLMS/Pan-European/High_Resolution_Layers/Ice/RLIE/2022/09/06/RLIE_20220906T130317_S2A_T28WEU_V102_0/thumbnail.png"}}}'
    job_final.fsc_path = "/eodata/HRSI/CLMS/Pan-European/High_Resolution_Layers/Snow/FSC/2022/09/06/FSC_20220906T130317_S2A_T28WEU_V102_0"
    job_final.rlie_path = "/eodata/HRSI/CLMS/Pan-European/High_Resolution_Layers/Ice/RLIE/2022/09/06/RLIE_20220906T130317_S2A_T28WEU_V102_0"
    job_final.measurement_date = "2022-09-06T13:03:11"
    job_final.l1c_sensing_time = "2022-09-06T13:03:17.59"
    job_final.l1c_esa_creation_date = "2022-09-06T18:21:27"
    job_final.l1c_esa_publication_date = "2022-09-06T19:31:30.984"
    job_final.l1c_dias_publication_date = "2022-09-06T20:12:38.655863"
    job_final.fsc_completion_date = "2022-09-13T12:53:16.246992"
    job_final.rlie_completion_date = "2022-09-13T12:53:16.246992"
    job_final.fsc_json_publication_date = "2022-09-13T12:53:19.973486"
    job_final.rlie_json_publication_date = "2022-09-13T12:53:19.977959"
    job_final.maja_mode = "init"
    job_final.maja_threads = 1
    job_final.maja_other_params = None
    job_final.maja_return_code = 0
    job_final.backward_reprocessing_run = False
    job_final.reprocessing_context = "None"
    job_final.name = "28WEU-2022-09-06"
    job_final.nomad_id = "si-processing/dispatch-1663070753-62cb0533"
    job_final.next_log_level =  logging.getLevelName("DEBUG")
    job_final.next_log_file_path = None
    job_final.print_to_orch = None
    
    # Applying the changes on the job on the DB
    job_final.patch(patch_foreign=True)
    sleep(1)
    
    # Setting the job at done
    job_final.post_new_status_change(JobStatus.done)

    
    ### Setting up a job to be finished with a L2A generated at Day 2 > Day 1 > Day 0
    # Extracting valid data
    product_identifier_ante = "/eodata/Sentinel-2/MSI/L1C/2022/08/29/S2B_MSIL1C_"\
                             "20220829T125259_N0400_R138_T28WEU_20220829T145121.SAFE"
    published_ante = DatetimeUtil.fromRfc3339("2022-08-29T15:52:51.869047Z")
    cloud_cover_ante = 77.250139208908
    snow_cover_ante = 0
    s2_product_ante = Sentinel2Product(product_identifier_ante, published_ante, cloud_cover_ante, snow_cover_ante)
    
    job_ante = FscRlieJob(tile_id=s2_product_ante.tile_id,
                    l1c_id=s2_product_ante.product_id,
                    l1c_path=s2_product_ante.product_path,
                    l1c_cloud_cover=s2_product_ante.cloud_cover,
                    l1c_snow_cover=s2_product_ante.snow_cover,
                    measurement_date=s2_product_ante.measurement_date,
                    l1c_esa_creation_date=s2_product_ante.esa_creation_date,
                    # There is also the 'update' param that we don't use.
                    l1c_dias_publication_date=s2_product_ante.dias_publication_date)
    
    # Set it to NRT for it to be computed ASAP
    job_ante.nrt = True
    # Set the log level used for this next job execution.
    job_ante.next_log_level =  logging.getLevelName("DEBUG")
    # Insert the job (fsc_rlie + parent job) into the database
    job_ante.post(post_foreign=True)
    # Set the job status to initialized
    job_ante.post_new_status_change(JobStatus.initialized)
    ### Configure the job in the middle yet created last
    job_ante, status_to_set, _ = job_ante.configure_single_job(temp_logger)

    # Assert it has taken the job prior to it as L2A source
    assert job_ante.job_id_for_last_valid_l2a == 1
    assert status_to_set == JobStatus.ready
