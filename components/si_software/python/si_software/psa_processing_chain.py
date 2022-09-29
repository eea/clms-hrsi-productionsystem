#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_common.yaml_parser import load_yaml
from si_geometry.geometry_functions import *
from si_geometry.get_valid_data_convex_hull import get_valid_data_convex_hull

from si_software.add_colortable_to_si_products import add_colortable_to_si_products
from si_utils.rewrite_cog import rewrite_cog
import multiprocessing
from si_software.add_quicklook import add_quicklook
from si_utils.rclone import Rclone


def fill_template_args(general_info, psa_tag, min_date, max_date, lonlat_minmax_dict):
    """Fill information within product XML file"""
    
    date_now = datetime.utcnow()
    
    #fill template information
    template_dict = dict()
    #copy information filled by CoSIMS team in the general_info.yaml file
    for key in ['helpdesk_email', 'product_version', 'pum_url', 'dias_url', 'dias_portal_name', 'report_date']:
        template_dict[key.upper()] = general_info[key]
    template_dict['VALIDATION_REPORT_FILENAME'] = 'hrsi-snow-qar'
    template_dict['PRODUCT_ID'] = psa_tag
    template_dict['PRODUCTION_DATE'] = date_now.strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
    template_dict['EDITION_DATE'] = date_now.strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
    template_dict['ACQUISITION_START'] = min_date.strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
    template_dict['ACQUISITION_STOP'] = max_date.strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
    
    #get lonlat border information from the R1.tif file in L2A data
    template_dict['WB_lon'] = '%s'%lonlat_minmax_dict['lonmin']
    template_dict['EB_lon'] = '%s'%lonlat_minmax_dict['lonmax']
    template_dict['SB_lat'] = '%s'%lonlat_minmax_dict['latmin']
    template_dict['NB_lat'] = '%s'%lonlat_minmax_dict['latmax']
    return template_dict



def generate_laea_psa_products(laea_dict, s2_eea39_dict, psa_product_dict, output_dir, temp_dir):
    
    start_time_general = time.time()
    
    template_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'templates', 'PSA_Metadata_LAEA.xml')
    general_info_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'templates', 'general_info.yaml')
    assert os.path.exists(template_file) and os.path.exists(general_info_file)
    general_info = load_yaml(general_info_file, env_vars=False)
    
    for fol in [output_dir, temp_dir]:
        os.makedirs(fol, exist_ok=True)
        
    #check that all input products are of the same year
    year = None
    for tile_id, product_path in psa_product_dict.items():
        year_loc = int(os.path.basename(product_path).split('_')[1][0:4])
        if year is None:
            year = year_loc
        else:
            assert year == year_loc
    
    missing_products = sorted(list(set(s2_eea39_dict.keys()) - set(psa_product_dict.keys())))
    psa_product_dict_corr = copy.deepcopy(psa_product_dict)
    if len(missing_products) > 0:
        raise Exception('Missing the following S2 tile PSA products:\n%s'%('\n'.join(['- %s'%el for el in missing_products])))

    existing_laea_products = [el for el in os.listdir(output_dir) if len(el.split('_'))==5]
    existing_laea_products = set([el.split('_')[-2] for el in existing_laea_products if el.split('_')[0] == 'PSA' and el.split('_')[-2] in set(laea_dict.keys())])
    psa_laea_product_dict = dict()
    
    
    for laea_tile_id, gdal_info_dict_laea in laea_dict.items():
        
        print('Processing LAEA tile %s'%laea_tile_id)
        
        if laea_tile_id in existing_laea_products:
            print('  => already generated, skipping...')
            continue
        
        start_time_laea_tile = time.time()
        
        #change template resolution to 20m
        gdal_info_dict_laea_20m = gdal_info_rescale(gdal_info_dict_laea, xRes=20, yRes=20)
        
        #create empty LAEA product (files are created but contain zeros) in temp folder
        temp_tag = 'PSA_LAEA_%s'%laea_tile_id
        temp_fol_loc = os.path.join(temp_dir, temp_tag)
        os.makedirs(temp_fol_loc)
        temp_file_psa = os.path.join(temp_fol_loc, 'PSA.tif')
        temp_file_qc = os.path.join(temp_fol_loc, 'QC.tif')

        #projections from each of the S2 tiles intersecting with LAEA tile
        laea_tile_raster_perimeter_obj = RasterPerimeter(gdal_info_dict_laea_20m)
        min_date_loc, max_date_loc = None, None
        for s2_tile_id, gdal_info_dict_s2 in s2_eea39_dict.items():
            
            start_time_s2_tile = time.time()
            
            if not RasterPerimeter(gdal_info_dict_s2).intersects(laea_tile_raster_perimeter_obj):
                continue
            s2_psa_product = psa_product_dict_corr[s2_tile_id]
            print('  -> intersection with S2 tile %s'%s2_tile_id)

            psa_s2_loc = os.path.join(s2_psa_product, os.path.basename(s2_psa_product) + '_PSA.tif')
            qc_s2_loc = os.path.join(s2_psa_product, os.path.basename(s2_psa_product) + '_QC.tif')
            assert os.path.exists(psa_s2_loc) and os.path.exists(qc_s2_loc)
            
            start_time = time.time()
            if min_date_loc is None: #1st step
                psa_laea_ar = reproject(psa_s2_loc, gdal_info_dict_laea_20m, temp_file_psa, return_array=True)
                qc_laea_ar = reproject(qc_s2_loc, gdal_info_dict_laea_20m, temp_file_qc, return_array=True)
                print('    reprojection1 : %s seconds'%(time.time()-start_time))
            else:

                start_time = time.time()
                psa_laea_ar_new = reproject(psa_s2_loc, gdal_info_dict_laea_20m, os.path.join(temp_fol_loc, 'psanew.tif'), return_array=True, remove_target_file=True)
                qc_laea_ar_new = reproject(qc_s2_loc, gdal_info_dict_laea_20m, os.path.join(temp_fol_loc, 'qcnew.tif'), return_array=True, remove_target_file=True)
                print('    reprojection : %s seconds'%(time.time()-start_time))
                
                apply_new = np.logical_or(psa_laea_ar == 255, np.logical_and(psa_laea_ar == 0, psa_laea_ar_new == 1))
                
                psa_laea_ar[apply_new] = psa_laea_ar_new[apply_new]
                qc_laea_ar[apply_new] = qc_laea_ar_new[apply_new]
            
            #get min,max dates
            min_date_prod = datetime.strptime(os.path.basename(s2_psa_product).split('_')[1].split('-')[0], '%Y%m%d')
            max_date_prod = min_date_prod+timedelta(float(os.path.basename(s2_psa_product).split('_')[1].split('-')[1]))
            if min_date_loc is None:
                min_date_loc, max_date_loc = min_date_prod, max_date_prod
            else:
                min_date_loc = min(min_date_loc, min_date_prod)
                max_date_loc = max(max_date_loc, max_date_prod)
                
            print('    aggregation of s2 tile finished: %s seconds total'%(time.time()-start_time_s2_tile))
            
            
            
        #write computed bands to PSA LAEA files
        write_band(temp_file_psa, psa_laea_ar)
        write_band(temp_file_qc, qc_laea_ar)
            
        product_tag = 'PSA_%s-%03d_S2_%s_%s'%(min_date_loc.strftime('%Y%m%d'), int(np.ceil((max_date_loc-min_date_loc).total_seconds()/(3600.*24.))), laea_tile_id, general_info['product_version'])
        template_dict = fill_template_args(general_info, product_tag, min_date_loc, max_date_loc, RasterPerimeter(temp_file_psa).get_lonlat_minmax())
        #psa LAEA metadata
        with open(template_file) as ds:
            psa_metadata_content = ds.read()
        for key, value in template_dict.items():
            psa_metadata_content = psa_metadata_content.replace('[%s]'%key, '%s'%value)
        with open('%s/MTD.xml'%temp_fol_loc, mode='w') as ds:
            ds.write(psa_metadata_content)
            
        #rename files to contain product ID
        for filename in os.listdir(temp_fol_loc):
            shutil.move(os.path.join(temp_fol_loc, filename), os.path.join(temp_fol_loc, product_tag + '_' + filename))
            
        #add color tables to tif files
        add_colortable_to_si_products(temp_fol_loc, product_tag=product_tag)
        
        #transform geotiff into COG
        rewrite_cog(temp_fol_loc, dest_path=os.path.join(output_dir, product_tag), verbose=1)
        
        #add quicklook
        add_quicklook(os.path.join(output_dir, product_tag), '_PSA.tif', reproject_to_wgs84=True)
        
        #add json output
        json_dict = {
            "collection_name": "HR-S&I",
            "resto": {
                "type": "Feature",
                "geometry": {
                    "wkt": get_valid_data_convex_hull(os.path.join(output_dir, product_tag, product_tag + '_PSA.tif'), \
                        valid_values=[0,1], proj_out='EPSG:4326', use_otb=True, temp_dir=temp_dir).wkt
                },
                "properties": {
                    "productIdentifier": product_tag,
                    "title": product_tag,
                    "resourceSize": compute_size_du(os.path.join(output_dir, product_tag)),
                    "organisationName": "EEA",
                    "startDate": min_date_loc.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    "completionDate": max_date_loc.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    "productType": "PSA_LAEA",
                    "resolution": 20,
                    "processingBaseline": product_tag.split('_')[-1],
                    "host_base": None,
                    "s3_bucket": None
                }}}
        with open(os.path.join(output_dir, product_tag, 'dias_catalog_submit.json'), mode='w') as ds:
            json.dump(json_dict, ds, ensure_ascii=True, indent=4)

        print('  => LAEA product %s generated after %s minutes'%(product_tag, (time.time()-start_time_laea_tile)/60.))
    
    print('  => LAEA product generation finished : %s hours total time'%((time.time()-start_time_general)/3600.))

    
    
    
def update_product_information_from_fsc_file(product_information, fsc_filename_first, fsc_filename_last):
    """Gather information from source FSC file to fill product information within product XML files"""
    
    #fill information from fsc_filename
    fsc_basename_first = os.path.basename(fsc_filename_first)
    fsc_basename_last = os.path.basename(fsc_filename_last)
    product_information['mission'] = fsc_basename_first.split('_')[2]
    product_information['measurement_date_first'] = datetime.strptime(fsc_basename_first.split('_')[1], '%Y%m%dT%H%M%S')
    product_information['measurement_date_last'] = datetime.strptime(fsc_basename_last.split('_')[1], '%Y%m%dT%H%M%S')
    product_information['measurement_span'] = (product_information['measurement_date_last'] - product_information['measurement_date_first']).total_seconds()/(24.*3600)
    product_information['tile_name'] = fsc_basename_first.split('_')[3][1:]
    product_information['tag'] = 'PSA_%s-%03d_S2_%s_%s'%(product_information['measurement_date_first'].strftime('%Y%m%d'), int(round(product_information['measurement_span'])), \
        'T' + product_information['tile_name'], product_information['general_info']['product_version'])
    
    #fill template information (used to fill FSC, RLIE, PSL and ARLIE templates)
    product_information['template'] = dict()
    #copy information filled by CoSIMS team in the general_info.yaml file
    for key in ['helpdesk_email', 'product_version', 'pum_url', 'dias_url', 'dias_portal_name', 'report_date']:
        product_information['template'][key.upper()] = product_information['general_info'][key]
    product_information['template']['VALIDATION_REPORT_FILENAME'] = 'hrsi-snow-qar'
    product_information['template']['PRODUCT_ID'] = product_information['tag']
    product_information['template']['PRODUCTION_DATE'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
    product_information['template']['EDITION_DATE'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
    product_information['template']['ACQUISITION_START'] = product_information['measurement_date_first'].strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
    product_information['template']['ACQUISITION_STOP'] = product_information['measurement_date_last'].strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
    
    #get lonlat border information from the R1.tif file in L2A data
    lonlat_minmax_dict = RasterPerimeter(fsc_filename_first).get_lonlat_minmax()
    product_information['template']['WB_lon'] = '%s'%lonlat_minmax_dict['lonmin']
    product_information['template']['EB_lon'] = '%s'%lonlat_minmax_dict['lonmax']
    product_information['template']['SB_lat'] = '%s'%lonlat_minmax_dict['latmin']
    product_information['template']['NB_lat'] = '%s'%lonlat_minmax_dict['latmax']
    return product_information
    
    
def psa_s2tile_processing(output_dir, temp_dir, fsc_files_in, rclone_config_file, persistent_snow_ratio=0.95):
    
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    temp_dir_session = tempfile.mkdtemp(dir=temp_dir, prefix='psas2')
    psa_product_output_dir = None
    
    try:
        
        fsc_temp_storage = os.path.join(temp_dir_session, 'fsctemp')
        os.makedirs(fsc_temp_storage)
        rclone_cmd = Rclone(config_file=rclone_config_file)
        fsc_files = []
        for fsc_file_loc in fsc_files_in:
            if ':' in fsc_file_loc:
                print('FSC file %s is stored on remote, downloading...'%os.path.basename(fsc_file_loc))
                t_start = datetime.utcnow()
                rclone_cmd.copy(fsc_file_loc, fsc_temp_storage)
                print(' -> %s'%(datetime.utcnow() - t_start))
                target_file_loc = os.path.join(fsc_temp_storage, os.path.basename(fsc_file_loc))
                assert os.path.exists(target_file_loc), 'file %s should have downloaded but was not found at destination'%os.path.basename(fsc_file_loc)
                fsc_files.append(target_file_loc)
            else:
                fsc_files.append(fsc_file_loc)
                
        product_information = update_product_information_from_fsc_file({'general_info': \
            load_yaml(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'templates', 'general_info.yaml'), env_vars=False)}, \
            fsc_files_in[0], fsc_files_in[-1])
        
        psa_product_output_dir = os.path.join(temp_dir_session, product_information['tag'])
        os.makedirs(psa_product_output_dir)
                
        psa_metadata_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'templates', 'PSA_Metadata.xml')
        
        n_fsc_files = len(fsc_files)
        if n_fsc_files < 20:
            print('WARNING for tile %s: for proper Permanent Snow Area assessment, at least 20 clear-sky pixels are needed, only %d overall present'%(product_information['tile_name'], n_fsc_files))



        ######################
        #compute PSA
        start_time = time.time()
        
        #iterate on FSCTOC files counting number of snow,cloud,valid(non-nan) pixels
        fsc_file_index = 0
        ds_in = gdal.Open(fsc_files[fsc_file_index])
        fsc_data = ds_in.GetRasterBand(1).ReadAsArray()
        ar_shape = np.shape(fsc_data)
        count_snow = np.zeros(ar_shape, dtype=np.int32)
        count_clear = np.zeros(ar_shape, dtype=np.int32)
        while(True):
            print('Adding data from file %s'%fsc_files[fsc_file_index])
            count_snow[np.logical_and(fsc_data > 0, fsc_data < 101)] += 1
            count_clear[fsc_data <= 100] += 1
            fsc_file_index += 1
            if fsc_file_index < n_fsc_files:
                ds_in = gdal.Open(fsc_files[fsc_file_index])
                assert ds_in.RasterCount == 1, 'fsc file %s expected to have a single band'%fsc_files[fsc_file_index]
                fsc_data = ds_in.GetRasterBand(1).ReadAsArray()
            else:
                break
                
        #create PSA
        driver = gdal.GetDriverByName('GTiff')
        
        #persistent snow area compute and write
        psa_data = np.zeros(ar_shape, dtype=np.uint8)
        psa_data[count_snow*1. >= count_clear*persistent_snow_ratio] = 1
        psa_data[count_clear == 0] = 255
        ds_out = driver.CreateCopy(os.path.join(psa_product_output_dir, product_information['tag'] + '_PSA.tif'), ds_in, 0)
        outband = ds_out.GetRasterBand(1)
        outband.DeleteNoDataValue()
        outband.SetNoDataValue(float(255))
        outband.WriteArray(psa_data)
        
        #persistent snow area QC layer compute and write
        psa_qc_data = np.zeros(ar_shape, dtype=np.uint8) #number of clear days >= 25 => highest quality
        psa_qc_data[np.logical_and(count_clear >=18, count_clear < 25)] = 1 #number of clear days in [18:25[ => lower quality
        psa_qc_data[np.logical_and(count_clear >=11, count_clear < 18)] = 2 #number of clear days in [11:18[ => decreasing quality
        psa_qc_data[count_clear < 11] = 3 #number of clear days in ]0:11[ => lowest quality
        psa_qc_data[count_clear == 0] = 255
        print('QC layer: 0:%d, 1:%d, 2:%d, 3:%d, nan:%d'%(np.sum(psa_data==0), np.sum(psa_data==1), np.sum(psa_data==2), np.sum(psa_data==3), np.sum(psa_data==255)))
        ds_out = driver.CreateCopy(os.path.join(psa_product_output_dir, product_information['tag'] + '_QC.tif'), ds_in, 0)
        outband = ds_out.GetRasterBand(1)
        outband.DeleteNoDataValue()
        outband.SetNoDataValue(float(255))
        outband.WriteArray(psa_qc_data)
        ds_out = None
        ds_in = None
        del ds_out, ds_in

        #psa metadata
        with open(psa_metadata_file) as ds:
            psa_metadata_content = ds.read()
        for key, value in product_information['template'].items():
            psa_metadata_content = psa_metadata_content.replace('[%s]'%key, '%s'%value)
        with open('%s/%s_MTD.xml'%(psa_product_output_dir, product_information['tag']), mode='w') as ds:
            ds.write(psa_metadata_content)
        
        #add color tables to tif files
        add_colortable_to_si_products(psa_product_output_dir)
        
        #transform geotiff into COG
        rewrite_cog(psa_product_output_dir, dest_path=os.path.join(output_dir, product_information['tag']), verbose=1)
        
        #add quicklook
        add_quicklook(os.path.join(output_dir, product_information['tag']), '_PSA.tif')
        
        #add json output
        json_dict = {
            "collection_name": "HR-S&I",
            "resto": {
                "type": "Feature",
                "geometry": {
                    "wkt": get_valid_data_convex_hull(os.path.join(output_dir, product_information['tag'], product_information['tag'] + '_PSA.tif'), \
                        valid_values=[0,1], proj_out='EPSG:4326', temp_dir=temp_dir_session).wkt
                },
                "properties": {
                    "productIdentifier": product_information['tag'],
                    "title": product_information['tag'],
                    "resourceSize": compute_size_du(os.path.join(output_dir, product_information['tag'])),
                    "organisationName": "EEA",
                    "startDate": product_information['measurement_date_first'].strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    "completionDate": product_information['measurement_date_last'].strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    "productType": "PSA_WGS84",
                    "resolution": 20,
                    "processingBaseline": product_information['tag'].split('_')[-1],
                    "host_base": None,
                    "s3_bucket": None
                }}}
        with open(os.path.join(output_dir, product_information['tag'], 'dias_catalog_submit.json'), mode='w') as ds:
            json.dump(json_dict, ds, ensure_ascii=True, indent=4)
        
        #add product to product file / product dict
        print('PSA processing on S2 tile %s successful, completed in %s seconds'%(product_information['tile_name'], time.time()-start_time))
        
    except:
        if psa_product_output_dir is not None: 
            if os.path.exists(psa_product_output_dir):
                shutil.rmtree(psa_product_output_dir)
        raise
    
    finally:
        
        #remove temp dir session
        shutil.rmtree(temp_dir_session)



def get_fsctoc_product_dict_on_s3_bucket(input_fsc_dir, start_date, end_date, rclone_config_file=None):
    rclone_cmd = Rclone(config_file=rclone_config_file)
    year_loc, month_loc = start_date.year, start_date.month
    file_list = []
    while(True):
        search_dir_loc = os.path.join(input_fsc_dir, '%04d'%year_loc, '%02d'%month_loc)
        print('Scanning %s ...'%search_dir_loc)
        t_start = datetime.utcnow()
        file_list_subpaths = rclone_cmd.search(search_dir_loc, expr='*FSCTOC.tif', silent_error=False)
        file_list += [os.path.join(search_dir_loc, el) for el in file_list_subpaths]
        print('  -> %s'%(datetime.utcnow() - t_start))
        month_loc += 1
        if month_loc > 12:
            year_loc += 1
            month_loc = 1
        if datetime(year_loc, month_loc, 1) > end_date:
            break
    fsctoc_product_dict = dict()
    for file_path in file_list:
        measurement_date = datetime.strptime(os.path.basename(file_path).split('_')[1], '%Y%m%dT%H%M%S')
        if (measurement_date > end_date) or (measurement_date < start_date):
            continue
        tile_id = os.path.basename(file_path).split('_')[3][1:]
        if tile_id not in fsctoc_product_dict:
            fsctoc_product_dict[tile_id] = []
        fsctoc_product_dict[tile_id].append(file_path)
    return fsctoc_product_dict



def download_fsctoc_files_efficiently(dico_in_tiled, store_dir, rclone_config_file=None):
    
    os.makedirs(store_dir, exist_ok=True)
    temp_dir_session = tempfile.mkdtemp(dir=store_dir, prefix='temp_')
    try:
        dico_in = dict()
        for value in dico_in_tiled.values():
            for val in value:
                dico_in[os.path.basename(val)] = val
        rclone_cmd = Rclone(config_file=rclone_config_file)
        #get commmon months
        common_month_paths = set(['/'.join(el.split('/')[0:-3]) for el in dico_in.values()])
        preexist_fsctoc = set(os.listdir(store_dir))
        for common_month_path in common_month_paths:
            year_loc, month_loc = common_month_path.split('/')[-2:]
            fsctoc_files_search = {el for el, val in dico_in.items() if common_month_path in val} - preexist_fsctoc
            if len(fsctoc_files_search) == 0:
                continue
            
            #get files from remote
            os.makedirs(temp_dir_session, exist_ok=True)
            print(' - Getting files from remote location %s ...'%common_month_path)
            t_start = datetime.utcnow()
            rclone_cmd.copy(common_month_path, temp_dir_session)
            print('    -> %s'%(datetime.utcnow() - t_start))
            fsctoc_files_loc = search_folder_structure(temp_dir_session, regexp='*FSCTOC.tif', maxdepth=5, object_type='f', case_sensible=True)
            fsctoc_files_loc = {os.path.basename(el): el for el in fsctoc_files_loc}
            missing_files = fsctoc_files_search - set(fsctoc_files_loc.keys())
            if len(missing_files) > 0:
                print('Missing FSCTOC files for month %04d/%02d'%(year_loc, month_loc))
                for el in sorted(list(missing_files)):
                    print(' - %s'%el)
                raise Exception('Missing FSCTOC files...')
            for key, value in fsctoc_files_loc.items():
                if key in fsctoc_files_search:
                    shutil.move(value, os.path.join(store_dir, key))
            shutil.rmtree(temp_dir_session)
            
        preexist_fsctoc = set(os.listdir(store_dir))
        dico_out = dict()
        for tile_id, values in dico_in_tiled.items():
            dico_out[tile_id] = []
            for val in values:
                el = os.path.basename(val)
                if el in preexist_fsctoc:
                    dico_out[tile_id].append(os.path.join(store_dir, el))
                else:
                    raise Exception('file %s not found in store_dir but it should be there : interference from other program or altorithm error'%el)
            
    finally:
        if os.path.exists(temp_dir_session):
            shutil.rmtree(temp_dir_session)
    

            
    return dico_out


def psa_processing_chain(output_dir, temp_dir, aoi_eea39_dir, start_date, end_date, input_fsc_dir, rclone_config_file=None, nprocs=4, max_ram=4096):
    
    #nprocs, memory used
    set_gdal_otb_itk_env_vars(nprocs=nprocs, max_ram=max_ram)
    
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    temp_dir_session = os.path.abspath(tempfile.mkdtemp(prefix='psa_', dir=temp_dir))
    
    laea_dict = json.load(open(os.path.join(aoi_eea39_dir, 'laea_tiles_eea39', 'laea_tiles_eea39_gdal_info.json')))
    s2_eea39_dict = json.load(open(os.path.join(aoi_eea39_dir, 's2tiles_eea39', 's2tiles_eea39_gdal_info.json')))
    
    
    #identify existing PSA S2
    existing_tiles_psa = set()
    for el in os.listdir(output_dir):
        if not os.path.isdir(os.path.join(output_dir, el)):
            continue
        if len(el.split('_')) != 5:
            continue
        if el.split('_')[3][1:] not in set(s2_eea39_dict.keys()):
            continue
        existing_tiles_psa.add(el.split('_')[3][1:])

    if not set(s2_eea39_dict.keys()).issubset(existing_tiles_psa):
        
        print('Processing PSA...')
        print('Getting FSCTOC product list...')
        
        fsc_product_dict_file = os.path.join(output_dir, 'fsctoc_product_dict.json')
        fsc_product_dict_remote_file = os.path.join(output_dir, 'fsctoc_product_dict_remote.json')
        if os.path.exists(fsc_product_dict_file):
            print('Reading FSCTOC list from pre-existing file %s'%fsc_product_dict_file)
            with open(fsc_product_dict_file) as ds:
                fsctoc_product_dict = json.load(ds)
        else:
            if ':' in input_fsc_dir:
                if os.path.exists(fsc_product_dict_remote_file):
                    with open(fsc_product_dict_remote_file) as ds:
                        fsctoc_product_dict = json.load(ds)
                else:
                    fsctoc_product_dict = get_fsctoc_product_dict_on_s3_bucket(input_fsc_dir, start_date, end_date, rclone_config_file=rclone_config_file)
                    for tile_id in set(fsctoc_product_dict.keys()) - set(s2_eea39_dict.keys()):
                        print(' - Removing tile %s which does not exist in s2_eea39_dict'%tile_id)
                        del fsctoc_product_dict[tile_id]
                    for tile_id in set(s2_eea39_dict.keys()) - set(fsctoc_product_dict.keys()):
                        print(' - Adding empty entry for tile %s which does not exist in fsctoc_product_dict'%tile_id)
                        fsctoc_product_dict[tile_id] = []
                    with open(fsc_product_dict_remote_file, mode='w') as ds:
                        json.dump(fsctoc_product_dict, ds, indent=4)
                        
                fsctoc_product_dict = download_fsctoc_files_efficiently(fsctoc_product_dict, os.path.join(output_dir, 'FSCTOC'), rclone_config_file=rclone_config_file)
                
            else:
                fsc_products = search_folder_structure(input_fsc_dir, regexp='FSC_*', maxdepth=5, object_type='d', case_sensible=True)
                fsctoc_product_dict = {tile_id: [] for tile_id in sorted(list(set(s2_eea39_dict.keys())))}
                for fsc_product in fsc_products:
                    measurement_date = datetime.strptime(os.path.basename(fsc_product).split('_')[1], '%Y%m%dT%H%M%S')
                    if (measurement_date > end_date) or (measurement_date < start_date):
                        continue
                    tile_id = os.path.basename(fsc_product).split('_')[3][1:]
                    if tile_id not in fsctoc_product_dict:
                        continue
                    fsctoc_product_dict[tile_id].append(os.path.join(fsc_product, os.path.basename(fsc_product) + '_FSCTOC.tif')) #append the FSCTOC file to the list
                    
            with open(fsc_product_dict_file, mode='w') as ds:
                json.dump(fsctoc_product_dict, ds, indent=4)

                
        #raise exceptions for tiles that do not meet sufficient number of FSC products
        for tile_id in fsctoc_product_dict.keys():
            if len(fsctoc_product_dict[tile_id]) < 5:
                raise Exception('insufficient number (<5) of products for tile %s'%tile_id)
            sorter_loc = list_argsort([datetime.strptime(os.path.basename(fsctoc_product).split('_')[1], '%Y%m%dT%H%M%S') for fsctoc_product in fsctoc_product_dict[tile_id]])
            fsctoc_product_dict[tile_id] = [fsctoc_product_dict[tile_id][ii] for ii in sorter_loc]
        
        
        #launch PSA processing
        print('Launching PSA processing...')
        tile_ids_process = set(fsctoc_product_dict.keys()) - existing_tiles_psa
        if nprocs <= 1:
            for tile_id in tqdm.tqdm(tile_ids_process):
                psa_s2tile_processing(output_dir, temp_dir, fsctoc_product_dict[tile_id], rclone_config_file)
        else:
            raise NotImplementedError('For some reason, gdal generates TiffWriteRawTile errors when running multiple tasks in parallel, so, for now, only launch on 1 proc.')
            pool = multiprocessing.Pool(processes=nprocs)
            _ = pool.starmap(psa_s2tile_processing, [(output_dir, temp_dir, fsctoc_product_dict[tile_id], rclone_config_file) for tile_id in tile_ids_process])
        

    #identify existing PSA S2
    existing_tiles_psa_dict, existing_tiles_psa_laea_dict = dict(), dict()
    for el in os.listdir(output_dir):
        if not os.path.isdir(os.path.join(output_dir, el)):
            continue
        if len(el.split('_')) != 5:
            continue
        if el.split('_')[3] in set(laea_dict.keys()):
            existing_tiles_psa_laea_dict[el.split('_')[3]] = os.path.join(output_dir, el)
        elif el.split('_')[3][1:] in set(s2_eea39_dict.keys()):
            existing_tiles_psa_dict[el.split('_')[3][1:]] = os.path.join(output_dir, el)
    assert set(s2_eea39_dict.keys()).issubset(set(existing_tiles_psa_dict.keys()))
    
    #PSA LAEA
    if not set(laea_dict.keys()).issubset(set(existing_tiles_psa_laea_dict.keys())):
        #launch PSA LAEA processing
        generate_laea_psa_products(laea_dict, s2_eea39_dict, existing_tiles_psa_dict, output_dir, temp_dir)
        



if __name__ == '__main__':
    

    import argparse
    parser = argparse.ArgumentParser(description='This script is used to launch Permanent Snow Area (S2 and LAEA) product generation for all EEA39 tiles')
    parser.add_argument("--output_dir", type=str, required=True, help='output directory')
    parser.add_argument("--temp_dir", type=str, help='temp directory')
    
    parser.add_argument("--year", type=int, help='year to compute PSA, will set start_date=Y/05/01 and end_date=Y/10/01')
    parser.add_argument("--start_date", type=str, help='start_date to compute PSA in Y-m-d format')
    parser.add_argument("--end_date", type=str, help='end_date to compute PSA in Y-m-d format. Warning: since the computation ends at Y-m-dT00:00:00, the end_date day is not included in the calculation.')

    parser.add_argument("--nprocs", type=int, default=4, help='number of procs to use, default is 4')
    parser.add_argument("--max_ram", type=int, default=4096, help='mac ram to use for gdal/OTB, default is 4096')
    
    parser.add_argument("--input_fsc_dir", type=str, help='input FSC directory containing tile_id/year/month/day/products or year/month/day/products subfolder structure')
    parser.add_argument("--aoi_eea39_dir", type=str, help='path to aoi_eea39_dir')
    parser.add_argument("--on_cnes_hpc", action='store_true', help="CNES reprocessing mode : completes some options by default")
    parser.add_argument("--rclone_config_file", type=str, help="rclone configuration file with necessary remote locations")
    args = parser.parse_args()
    
    #dates
    if args.year is None:
        assert args.start_date is not None
        assert args.end_date is not None
        args.start_date = datetime.strptime(args.start_date + 'T000000', '%Y-%m-%dT%H%M%S')
        args.end_date = datetime.strptime(args.end_date + 'T000000', '%Y-%m-%dT%H%M%S')
    else:
        assert args.start_date is None
        assert args.end_date is None
        args.start_date, args.end_date = datetime(args.year,5,1), datetime(args.year,10,1)

    #temp dir
    if args.temp_dir is None:
        assert 'TMPDIR' in os.environ
        args.temp_dir = os.environ['TMPDIR']
    
    #cnes HPC set values  
    if args.on_cnes_hpc:
        from si_reprocessing import install_cnes
        if args.aoi_eea39_dir is None:
            args.aoi_eea39_dir = os.path.join(install_cnes.reprocessing_static_data_dir, 'hidden_value', 'AOI_EEA39')
        if (args.input_fsc_dir is None):
            args.input_fsc_dir = os.path.join(install_cnes.reprocessing_cnes_hrsi_storage_dir, 'Snow', 'FSC')
    else:
        assert args.aoi_eea39_dir is not None, 'aoi_eea39_dir must be defined'
        

    psa_processing_chain(args.output_dir, args.temp_dir, args.aoi_eea39_dir, args.start_date, args.end_date, args.input_fsc_dir, rclone_config_file=args.rclone_config_file, \
        nprocs=args.nprocs, max_ram=args.max_ram)
