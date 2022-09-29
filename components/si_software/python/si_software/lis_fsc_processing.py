#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_common.follow_process import execute_commands, dump_execution_dict_to_directory

from si_geometry.geometry_functions import *

from si_utils.rewrite_cog import rewrite_cog

from si_software.add_colortable_to_si_products import add_colortable_to_si_products
from si_software.add_quicklook import add_quicklook

   

    
def edit_lis_fsc_qc_layers(lis_input_folder, l2a_path, water_mask_path, tcd_path, output_folder=None):
    
    maja_product_tag = os.path.basename(l2a_path)
    lis_product_tag = os.path.basename(lis_input_folder)
    if output_folder is None:
        output_folder = lis_input_folder
    os.makedirs(output_folder, exist_ok=True)
    fsc_toc_file = check_path(os.path.join(lis_input_folder, '%s_FSCTOC.tif'%lis_product_tag))
    fsc_og_file = check_path(os.path.join(lis_input_folder, '%s_FSCOG.tif'%lis_product_tag))
    maja_cloud_mask_file = check_path(os.path.join(l2a_path, 'MASKS', '%s_CLM_R2.tif'%maja_product_tag))
    geophysical_mask_file = check_path(os.path.join(l2a_path, 'MASKS', '%s_MG2_R2.tif'%maja_product_tag))
    raster_gdal_info = gdal.Info(fsc_toc_file, format='json')
    
    #cloud : there used to be a specific cloud processing but instead we are just copying the MAJA cloud mask
    output_cloud_file = '%s/%s_CLD.tif'%(output_folder, lis_product_tag)
    copy_original(maja_cloud_mask_file, output_cloud_file)
    
    #expert flags
    source_list = []
    #bit 0: MAJA sun too low for an accurate slope correction
    #bit 1: MAJA sun tangent
    source_list += [{'sources': [{'filepath': geophysical_mask_file, 'bandnumber': 1, 'unpack_bits': True}], \
        'operation': {0: 'A0[:,:,6]', 1: 'A0[:,:,7]'}}]
    #bit 2: water mask
    source_list += [{'sources': [{'filepath': water_mask_path, 'bandnumber': 1, 'unpack_bits': False}], \
        'operation': {2: 'A0==1'}}]
    #bit 3: tree cover density > 90%
    source_list += [{'sources': [{'filepath': tcd_path, 'bandnumber': 1, 'unpack_bits': False}], \
        'operation': {3: 'np.logical_and(A0<101,A0>90)'}}]
    #bit 4: snow detected under thin clouds
    source_list += [{'sources': [{'filepath': maja_cloud_mask_file, 'bandnumber': 1, 'unpack_bits': False}, {'filepath': fsc_toc_file, 'bandnumber': 1, 'unpack_bits': False}], \
        'operation': {4: '(A0>0)*(A1>0)*(A1<101)'}}]
    #bit 5: tree cover density undefined or unavailable
    source_list += [{'sources': [{'filepath': tcd_path, 'bandnumber': 1, 'unpack_bits': False}], \
        'operation': {5: 'A0>100'}}]
    output_expert_file = '%s/%s_QCFLAGS.tif'%(output_folder, lis_product_tag)
    bit_bandmath(output_expert_file, raster_gdal_info, [source_list], compress=False, add_overviews=False, use_default_cosims_config=False)
    
    #QC layer top of canopy
    #[0: highest quality, 1: lower quality, 2: decreasing quality, 3: lowest quality, 205: cloud mask, 255: no data]
    source_list = []
    source_list += [{'sources': [{'filepath': output_expert_file, 'bandnumber': 1, 'unpack_bits': True}], \
        'operation': 'np.minimum(B*0+3,(4-np.maximum(B*0, (100.-30.*A0[:,:,1]-50.*A0[:,:,0]-25.*A0[:,:,4]-25.*A0[:,:,2])/25.))).astype(np.uint8)'}]
    #values 205 and 255 from FSCTOC snow product
    source_list += [{'sources': [{'filepath': fsc_toc_file, 'bandnumber': 1, 'unpack_bits': False}], \
        'operation': 'B*(A0!=205)*(A0!=255) + 205*(A0==205) + 255*(A0==255)'}]
    output_qc_toc_file = '%s/%s_QCTOC.tif'%(output_folder, lis_product_tag)
    bit_bandmath(output_qc_toc_file, raster_gdal_info, [source_list], no_data_values_per_band=[np.uint8(255)], compress=False, add_overviews=False, use_default_cosims_config=False)
    
    #QC layer on ground
    #[0: highest quality, 1: lower quality, 2: decreasing quality, 3: lowest quality, 205: cloud mask, 255: no data]
    source_list = []
    source_list += [{'sources': [{'filepath': output_expert_file, 'bandnumber': 1, 'unpack_bits': True}, {'filepath': tcd_path, 'bandnumber': 1, 'unpack_bits': False}], \
        'operation': 'np.minimum(B*0+3,(4-np.maximum(B*0, (100.-30.*A0[:,:,1]-50.*A0[:,:,0]-25.*A0[:,:,4]-25.*A0[:,:,2]-80.*A1)/25.))).astype(np.uint8)'}]
    #values 205 and 255 from FSCOG snow product
    source_list += [{'sources': [{'filepath': fsc_og_file, 'bandnumber': 1, 'unpack_bits': False}], \
        'operation': 'B*(A0!=205)*(A0!=255) + 205*(A0==205) + 255*(A0==255)'}]
    output_qc_og_file = '%s/%s_QCOG.tif'%(output_folder, lis_product_tag)
    bit_bandmath(output_qc_og_file, raster_gdal_info, [source_list], no_data_values_per_band=[np.uint8(255)], compress=False, add_overviews=False, use_default_cosims_config=False)
    



def fsc_product_final_editing(lis_product_input_dir, lis_product_output_dir, l2a_path, water_mask_path, tcd_path, fsc_metadata_file, product_information, apply_dem_mask_file=None):
    
    product_id = os.path.basename(lis_product_output_dir)
    
    
    #copy only selected tif files to output directory with proper filenames, TIFF DEFLATE zlevel 4 compression, and TILED
    name_conversion_dict = {'LIS_FSCOG.TIF': 'FSCOG.tif', 'LIS_FSCTOC.TIF': 'FSCTOC.tif', 'LIS_NDSI.TIF': 'NDSI.tif'}
    lis_files = set(os.listdir(lis_product_input_dir))
    for filename in lis_files - set(name_conversion_dict.keys()):
        os.unlink('%s/%s'%(lis_product_input_dir, filename))
    missing = set(name_conversion_dict.keys()) - lis_files
    if len(missing) > 0:
        raise RuntimeInputFileError('Missing FSC files:%s\n'%('\n'.join([' - %s'%el for el in sorted(list(missing))])))
    for filename in name_conversion_dict:
        shutil.move(os.path.join(lis_product_input_dir, filename), os.path.join(lis_product_input_dir, name_conversion_dict[filename]))
        if apply_dem_mask_file is not None:
            apply_dem_mask(os.path.join(lis_product_input_dir, name_conversion_dict[filename]), apply_dem_mask_file)
    
    input_product_tagged_dir = os.path.join(lis_product_input_dir, product_id)
    os.makedirs(input_product_tagged_dir)
    for filename in os.listdir(lis_product_input_dir):
        if not os.path.isfile(os.path.join(lis_product_input_dir, filename)):
            continue
        shutil.move(os.path.join(lis_product_input_dir, filename), os.path.join(input_product_tagged_dir, product_id + '_' + filename))
    
    #compute additional files (qc layers for top canopy and on ground fsc products as well as cloud mask (imported from MAJA and LIS) and an expert flag file (QCFLAGS.TIF)
    print(' -> adding qc layers and copying cloud file')
    edit_lis_fsc_qc_layers(input_product_tagged_dir, l2a_path, water_mask_path, tcd_path)
    
    #add color tables to tif files
    print(' -> adding colortable')
    add_colortable_to_si_products(input_product_tagged_dir, product_tag=product_id)
    
    #transform geotiff into COG
    print(' -> adding overviews and internal compression')
    rewrite_cog(input_product_tagged_dir, dest_path=lis_product_output_dir, verbose=1)
    
    #add quicklooks
    print(' -> adding quicklook')
    add_quicklook(lis_product_output_dir, '_FSCTOC.tif')
    
    print(' -> writing XML and JSON file')
    #fsc metadata
    with open(fsc_metadata_file) as ds:
        fsc_metadata_content = ds.read()
    for key, value in product_information['template'].items():
        fsc_metadata_content = fsc_metadata_content.replace('[%s]'%key, '%s'%value)
    with open('%s/%s_MTD.xml'%(lis_product_output_dir, product_id), mode='w') as ds:
        ds.write(fsc_metadata_content)
    
    json_dict = {
        "collection_name": "HR-S&I",
        "resto": {
            "type": "Feature",
            "geometry": {
                "wkt": product_information['wekeo_geom']
            },
            "properties": {
                "productIdentifier": product_id,
                "title": product_id,
                "resourceSize": compute_size_du(lis_product_output_dir),
                "organisationName": "EEA",
                "startDate": product_information['measurement_date'].strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                "completionDate": product_information['measurement_date'].strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                "productType": "FSC",
                "resolution": 20,
                "cloudCover": '%s'%product_information['cloud_cover_percent'], # get cloud cover percent
                "processingBaseline": product_id.split('_')[-1],
                "host_base": None,
                "s3_bucket": None
            }}}
            
    with open('%s/dias_catalog_submit.json'%lis_product_output_dir, mode='w') as ds:
        json.dump(json_dict, ds, ensure_ascii=True, indent=4)



def get_dem_20m_lis_file_from_dem_dir(dem_dir):
    if '.DBL.DIR' in dem_dir:
        dtm_file = os.path.join(dem_dir, 'dem_20m.tif')
    else:
        subdirs = [el for el in os.listdir(dem_dir) if '.DBL.DIR' in el]
        if len(subdirs) != 1:
            raise MainInputFileError('in search for DEM .DBL.DIR, found %d match instead of 1'%len(subdirs))
        dtm_file = os.path.join(dem_dir, subdirs[0], 'dem_20m.tif')
    if not os.path.exists(dtm_file):
        raise MainInputFileError('Could not find dtm file at %s'%dtm_file)
    return dtm_file




def make_lis_script(lis_in_dir, lis_out_dir, dem_dir, l2a_file, tcd_file, nprocs_lis):
    
    cmd_file = '%s/run_lis.sh'%lis_in_dir
    dtm_file = get_dem_20m_lis_file_from_dem_dir(dem_dir)

    if not os.path.exists(tcd_file):
        raise MainInputFileError('tree cover density file missing for LIS at path %s'%tcd_file)

    #add build_json command
    cmds = "\nbuild_json.py -nb_threads %d -dem %s -log false -fsc %s -cosims_mode "%(nprocs_lis, dtm_file, tcd_file)
    cmds += "%s %s\n"%(l2a_file, lis_out_dir)
    #add run_snow_detector.py command
    cmds += "run_snow_detector.py %s/param_test.json\n"%lis_out_dir
    
    #write script to file and make it executable
    with open(cmd_file, mode='w') as ds:
        ds.write(cmds)
    os.system('chmod u+x %s'%cmd_file)
    
    return ['sh', '%s'%cmd_file]
    
    
    
    
def lis_fsc_processing(main_info):
    
    main_info.update_processing_status(new_value='lis_preprocessing')
    main_info.logger_info('')
    main_info.logger_info('')
    main_info.logger_info('##########################################################################################')
    main_info.logger_info('LIS')
    main_info.logger_info('')
    main_info.logger_info('LIS preprocessing...')
    
    dico = main_info.input_parameters
    product_information = main_info.product_information
    l2a_file = main_info.l2a_file_work_path

    ######################
    #prepare inputs
    lis_temp_dir = os.path.join(main_info.main_temp_dir, 'lis')
    for fol in ['in', 'out']:
        os.system('mkdir -p %s/%s'%(lis_temp_dir, fol))

    ######################
    #execute LIS
    #make shell script for LIS (LIS requires additional environment variables to be set as well as additions to PATH and PYTHONPATH that we don't want to have in the general csi_si_software context)
    cmd_lis = {'cmd': make_lis_script('%s/in'%lis_temp_dir, '%s/out'%lis_temp_dir, dico['dem_dir'], l2a_file, dico['lis']['tree_cover_density'], dico['nprocs']), \
        'stdout_write_objects': [main_info.get_subtask_logger_writer(prefix='LIS stdout')], 'stderr_write_objects': [main_info.get_subtask_logger_writer(prefix='LIS stderr')]}
    #LIS launch job
    main_info.update_processing_status(new_value='lis_start')
    execution_dict = execute_commands({'lis': cmd_lis}, maxtime_seconds=dico['lis']['max_processing_time'], scan_dt=1, verbose=0)['lis']


    ######################
    #LIS postprocessing    
    main_info.update_processing_status(new_value='lis_postprocessing')
    lis_product_dir = '%s/out/LIS_PRODUCTS'%lis_temp_dir
    lis_successful = False
    #check LIS success/failure
    dump_execution_dict_to_directory(execution_dict, '%s/logs/lis'%main_info.main_output_dir)
    if execution_dict['returncode'] == 0:
        #check LIS product output
        lis_successful = len(os.listdir(lis_product_dir)) > 0
        if not lis_successful:
            main_info.update_processing_status(new_value='lis_noproduct')
            raise Exception('LIS returned without error %s after %s seconds => terminating csi_si_software'%execution_dict['execution_time'])
        else:
            #saving LIS product file
            dir_save = product_information['template']['FSC_PRODUCT_ID']
            fsc_product_final_editing(lis_product_dir, '%s/data/%s'%(main_info.main_output_dir, dir_save), l2a_file, dico['lis']['water_mask'], dico['lis']['tree_cover_density'], \
                main_info.templates['fsc'], product_information, apply_dem_mask_file=os.path.join(dico['dem_dir'], os.path.basename(dico['dem_dir']) + '.DBL.DIR', 'dem_20m.tif'))
            main_info.update_product_dict(new_product_keyval_tuple=('fsc', dir_save))
            main_info.logger_info('LIS processing successful, completed in %s seconds'%execution_dict['execution_time'])
    elif execution_dict['exceeded_time']:
        main_info.update_processing_status(new_value='lis_expired')
        raise CodedException('LIS calculation exceeded %s seconds => terminating csi_si_software'%dico['exec_time_max'], exitcode=exitcodes.subprocess_user_defined_timeout)
    else:
        main_info.update_processing_status(new_value='lis_failed')
        raise CodedException('LIS returned with error %s after %s seconds => terminating csi_si_software\n%s\n'%(execution_dict['returncode'], \
            execution_dict['execution_time'], '\n'.join(execution_dict['stderr'])), exitcode=fsc_rlie_exitcodes.lis_unknown_error)


def simple_lis_cosims_processing(output_dir, l2a_path, dem_dir, water_mask_path=None, tcd_path=None, product_mode_overide=1, nprocs=1):
    
    os.makedirs(output_dir, exist_ok=True)
    lis_temp_dir = tempfile.mkdtemp(dir=output_dir)
    try:
        #get product information from git repo and L2A file
        print('Getting si_software information from source files...')
        product_information = dict()
        fsc_template = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'templates', 'FSC_Metadata.xml')
        assert os.path.exists(fsc_template), 'could not find FSC template file on si_software install directory at %s'%fsc_template
        product_information = dict()
        from si_common.yaml_parser import load_yaml
        general_info_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'templates', 'general_info.yaml')
        assert os.path.exists(general_info_file), 'could not find general info file on si_software install directory at %s'%general_info_file
        product_information['general_info'] = load_yaml(general_info_file, env_vars=False)
        product_information['product_mode_overide'] = product_mode_overide
        product_information['production_date'] = datetime.utcnow()
        
        print('Getting L2A product information from L2A file...')
        from si_software.maja_l2a_processing import update_product_information_from_l2a_file
        product_information = update_product_information_from_l2a_file(product_information, l2a_path, temp_dir=lis_temp_dir)
        
        model_l2a_tif = os.path.join(l2a_path, os.path.basename(l2a_path) + '_FRE_B11.tif')
        if water_mask_path is None:
            print('WARNING: no water mask was provided and the water mask in the output QCFLAGS file will therefore be empty (all land) !')
            water_mask_path = os.path.join(lis_temp_dir, 'water_mask.tif')
            initialize_raster(gdal.Info(model_l2a_tif, format='json'), water_mask_path, nbands=1, dtype='u1', \
                nodata_value=None, fill_all_value=np.uint8(0), random_dict=None, compress=True)
        if tcd_path is None:
            print('WARNING: no tree cover density file was provided, so we assume it is 0 everywhere i.e. no trees and FSCOG, QC and QCFLAGS files will be unreliable !')
            tcd_path = os.path.join(lis_temp_dir, 'tcd.tif')
            initialize_raster(gdal.Info(model_l2a_tif, format='json'), tcd_path, nbands=1, dtype='u1', \
                nodata_value=None, fill_all_value=np.uint8(0), random_dict=None, compress=True)
        
        print('Building LIS command...')
        os.makedirs(os.path.join(lis_temp_dir, 'in'))
        os.makedirs(os.path.join(lis_temp_dir, 'out'))
        cmd = make_lis_script(os.path.join(lis_temp_dir, 'in'), os.path.join(lis_temp_dir, 'out'), dem_dir, l2a_path, tcd_path, nprocs)
        
        print('Launching LIS...\n%s'%(' '.join(cmd)))
        subprocess.check_call(cmd)
        
        print('Checking LIS outputs...')
        lis_product_dir = os.path.join(lis_temp_dir, 'out', 'LIS_PRODUCTS')
        assert len(os.listdir(lis_product_dir)) > 0
        
        print('Making COSIMS FSC product...')
        fsc_product_id = product_information['template']['FSC_PRODUCT_ID']
        if os.path.exists(os.path.join(output_dir, fsc_product_id)):
            print('  -> Removing preexisting product at %s'%os.path.join(output_dir, fsc_product_id))
            shutil.rmtree(os.path.join(output_dir, fsc_product_id))
        fsc_product_final_editing(lis_product_dir, os.path.join(output_dir, fsc_product_id), l2a_path, water_mask_path, tcd_path, fsc_template, product_information, \
            apply_dem_mask_file=get_dem_20m_lis_file_from_dem_dir(dem_dir))
    finally:
        if os.path.exists(lis_temp_dir):
            shutil.rmtree(lis_temp_dir)

    
    
if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='This script is used to process FSC from an L2A tile')
    parser.add_argument("--output_dir", type=str, required=True, help='output directory')
    parser.add_argument("--l2a", type=str, required=True, help='L2A path')
    parser.add_argument("--dem_dir", type=str, required=True, help='dem directory')
    parser.add_argument("--water_mask_path", type=str, help='water mask path')
    parser.add_argument("--tcd_path", type=str, help='tree cover density path')
    parser.add_argument("--product_mode_overide", type=int, default=1, help='product_mode_overide. 1 for nominal, 0 for degraded quality.')
    parser.add_argument("--nprocs", type=int, default=1, help='nprocs')
    args = parser.parse_args()
    

    simple_lis_cosims_processing(args.output_dir, args.l2a, args.dem_dir, water_mask_path=args.water_mask_path, \
        tcd_path=args.tcd_path, product_mode_overide=args.product_mode_overide, nprocs=args.nprocs)
    
    
