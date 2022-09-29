#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_common.follow_process import execute_commands, dump_execution_dict_to_directory

from si_geometry.geometry_functions import *

from si_utils.rewrite_cog import rewrite_cog

from si_software.add_colortable_to_si_products import add_colortable_to_si_products
from si_software.add_quicklook import add_quicklook


def ice_product_final_editing(ice_product_input_dir, ice_product_output_dir, product_information, apply_dem_mask_file=None):
    
    files_produced = os.listdir(ice_product_input_dir)
    rlie_file = [el for el in files_produced if 'RLIE.tif' in el]
    assert len(rlie_file) == 1
    rlie_file = rlie_file[0]
    product_id = '_'.join(rlie_file.split('_')[0:-1])
    files_checked = []
    for expected_sufix in ['RLIE.tif', 'QC.tif', 'QC-FLAGS.tif', 'MTD.xml']:
        expected_sufix_corr = expected_sufix.replace('-','')
        file_path = os.path.join(ice_product_input_dir, product_id + '_' + expected_sufix)
        assert os.path.exists(file_path), 'missing %s file in ICE RLIE output'%expected_sufix
        files_checked.append(product_id + '_' + expected_sufix)
        shutil.move(file_path, os.path.join(ice_product_input_dir, expected_sufix_corr))
        if (apply_dem_mask_file is not None) and ('.tif' in expected_sufix):
            apply_dem_mask(os.path.join(ice_product_input_dir, expected_sufix_corr), apply_dem_mask_file)
    for filename in files_produced:
        if filename not in files_checked:
            os.unlink(os.path.join(ice_product_input_dir, filename))
            
    product_id = os.path.basename(ice_product_output_dir)
    
    #rename files to contain product ID
    input_product_tagged_dir = os.path.join(ice_product_input_dir, product_id)
    os.makedirs(input_product_tagged_dir)
    for filename in os.listdir(ice_product_input_dir):
        if not os.path.isfile(os.path.join(ice_product_input_dir, filename)):
            continue
        shutil.move(os.path.join(ice_product_input_dir, filename), os.path.join(input_product_tagged_dir, product_id + '_' + filename))
    
    #add color tables to tif files
    print(' -> adding colortable')
    add_colortable_to_si_products(input_product_tagged_dir, product_tag=product_id)
    
    #transform geotiff into COG
    print(' -> adding overviews and internal compression')
    rewrite_cog(input_product_tagged_dir, dest_path=ice_product_output_dir, verbose=1)
    
    #add quicklook
    print(' -> adding quicklook')
    add_quicklook(ice_product_output_dir, '_RLIE.tif')
    
    print(' -> writing JSON file')
    json_dict = {
        "collection_name": "HR-S&I",
        "resto": {
            "type": "Feature",
            "geometry": {
                "wkt": product_information['wekeo_geom']
            },
            "properties": {
                "productIdentifier": None,
                "title": product_id,
                "resourceSize": compute_size_du(ice_product_output_dir),
                "organisationName": "EEA",
                "startDate": product_information['measurement_date'].strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                "completionDate": product_information['measurement_date'].strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                "productType": "RLIE",
                "resolution": 20,
                "cloudCover": '%s'%product_information['cloud_cover_percent'], # get cloud cover percent
                "processingBaseline": product_id.split('_')[-1],
                "host_base": None,
                "s3_bucket": None
            }}}
            
    with open('%s/dias_catalog_submit.json'%ice_product_output_dir, mode='w') as ds:
        json.dump(json_dict, ds, ensure_ascii=True, indent=4)
    
    
    
def make_ice_script(dico_ice, ice_temp_dir, l2a_file, rlie_metadata_file, product_information, nprocs_ice):
    
    #get l2a_xml path
    l2a_xml = ['%s/%s'%(l2a_file, el) for el in os.listdir(l2a_file) if '.xml' in el]
    if len(l2a_xml) == 1:
        l2a_xml = os.path.realpath(l2a_xml[0])
    else:
        raise RuntimeInputFileError('Could not find L2A xml file path in %s'%l2a_file)
        
    
    #ice program
    program_path = shutil.which('ProcessRiverIce')
    program_dir_path = os.path.dirname(program_path)
    
    #graph file
    graph_dir_path = os.path.join(program_dir_path, 'graphs')
    graph_files = [el for el in os.listdir(graph_dir_path) if el.split('.')[-1] == 'xml']
    if len(graph_files) == 0:
        raise MainInputFileError('no graph file found for Ice software in folder %s'%graph_dir_path)
    if len(graph_files) > 1:
        raise MainInputFileError('multiple graph files found for Ice software in folder %s'%graph_dir_path)
    graph_file = os.path.join(graph_dir_path, graph_files[0])
    del graph_dir_path, graph_files
        
    #edit appsettings.json file
    dico_json = {"Configuration" : {"SnapPath": shutil.which('gpt'), \
            "GraphPath": graph_file, \
            "RlieMetadataTemplatePath": rlie_metadata_file, \
            "MaxThreads": nprocs_ice, \
            "HelpDeskEmail": product_information['template']['helpdesk_email'.upper()], \
            "ProductVersion": product_information['template']['product_version'.upper()], \
            "PumUrl": product_information['template']['pum_url'.upper()], \
            "DiasUrl": product_information['template']['dias_url'.upper()], \
            "DiasPortalName": product_information['template']['dias_portal_name'.upper()], \
            "ValidationReportFilename": 'hrsi-ice-qar', \
            "ValidationReportDate": product_information['template']['report_date'.upper()]}}
    dico_json['Serilog'] = {'MinimumLevel': 'Debug', 'WriteTo': [{'Name': 'Async', 'Args': {'configure': [{'Name': 'Console', \
        "outputTemplate": "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level}] [{SourceContext}] [{EventId}] {Message}{NewLine}{Exception}"}]}}]}
    with open('%s/appsettings.json'%ice_temp_dir, mode='w') as ds:
        json.dump(dico_json, ds)
    
    cmd = [program_path, 'RLIE', l2a_xml, dico_ice['river_shapefile'], dico_ice['hrl_flags_file'], '%s/out'%ice_temp_dir, '%s/appsettings.json'%ice_temp_dir]
    return cmd
    
    

        
    
    
def ice_rlie_processing(main_info):
    
    main_info.update_processing_status(new_value='ice_preprocessing')
    main_info.logger_info('')
    main_info.logger_info('')
    main_info.logger_info('##########################################################################################')
    main_info.logger_info('ICE')
    main_info.logger_info('')
    main_info.logger_info('ICE preprocessing...')
    
    dico = main_info.input_parameters
    if not dico['ice']['generate_product']:
        raise MainArgError('this function should not have been called if ice:generate_product input parameter is set to false')
    product_information = main_info.product_information
    l2a_file = main_info.l2a_file_work_path

    ######################
    #prepare inputs
    ice_temp_dir = os.path.join(main_info.main_temp_dir, 'ice')
    os.system('mkdir -p %s'%ice_temp_dir)
            
            
    ######################
    #execute ICE
    cmd_ice = {'cmd': make_ice_script(dico['ice'], ice_temp_dir, l2a_file, main_info.templates['rlie'], product_information, dico['nprocs']), \
        'stdout_write_objects': [main_info.get_subtask_logger_writer(prefix='ICE stdout')], 'stderr_write_objects': [main_info.get_subtask_logger_writer(prefix='ICE stderr')]}
    #ICE launch job
    main_info.update_processing_status(new_value='ice_start')
    execution_dict = execute_commands({'ice': cmd_ice}, maxtime_seconds=dico['ice']['max_processing_time'], scan_dt=1, verbose=0)['ice']
    
    
    ######################
    #ICE postprocessing    
    main_info.update_processing_status(new_value='ice_postprocessing')
    ice_product_dir = '%s/out'%ice_temp_dir
    #check ICE success/failure
    dump_execution_dict_to_directory(execution_dict, '%s/logs/ice'%main_info.main_output_dir)
    if execution_dict['returncode'] == 0:
        if any(['ERROR' in line or 'ERR]' in line for line in execution_dict['stdout'] + execution_dict['stderr']]):
            execution_dict['returncode'] = -1
    if execution_dict['returncode'] == 0:
        #saving ICE product file
        dir_save = product_information['template']['RLIE_PRODUCT_ID']
        ice_product_final_editing(ice_product_dir, '%s/data/%s'%(main_info.main_output_dir, dir_save), product_information)
        main_info.update_product_dict(new_product_keyval_tuple=('rlie', dir_save))
        main_info.logger_info('ICE processing successful, completed in %s seconds'%execution_dict['execution_time'])
    elif execution_dict['exceeded_time']:
        main_info.update_processing_status(new_value='ice_expired')
        raise CodedException('ICE calculation exceeded %s seconds => terminating csi_si_software'%dico['exec_time_max'], exitcode=exitcodes.subprocess_user_defined_timeout)
    else:
        main_info.update_processing_status(new_value='ice_failed')
        raise CodedException('ICE returned with error %s after %s seconds => terminating csi_si_software\n%s\n'%(execution_dict['returncode'], \
            execution_dict['execution_time'], '\n'.join(execution_dict['stderr'])), exitcode=fsc_rlie_exitcodes.ice_unknown_error)
            

    

