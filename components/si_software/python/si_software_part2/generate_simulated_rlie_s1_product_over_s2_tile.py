#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_common.yaml_parser import load_yaml, dump_yaml
from si_common.follow_process import *
from si_geometry.geometry_functions import *
from si_geometry.get_valid_data_convex_hull import get_valid_data_convex_hull
import si_software.si_logger as si_logger
from si_software.add_quicklook import add_quicklook
from si_software_part2.s1_utils import *
from ice_s1_product_final_editing import ice_s1_product_final_editing



def generate_simulated_rlie_s1_product_over_s2_tile(output_dir, product_name, dem_file_loc, euhydro_shapefile, json_inputs=None, after_si_software_mode=False, temp_dir=None, product_geometry=None):
    
    assert len(product_name.split('_')) == 6
    assert not os.path.exists(os.path.join(output_dir, product_name))
    
    #main output dir
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    #temp dir
    if temp_dir is not None:
        os.makedirs(temp_dir, exist_ok=True)
    temp_dir_session = tempfile.mkdtemp(dir=temp_dir, prefix='fakerlie')
    
    try:
        product_dir = os.path.join(temp_dir_session, product_name)
        os.makedirs(product_dir)
        
        datetime_now = datetime.utcnow()
        acquisition_start_date = datetime.strptime(product_name.split('_')[1], '%Y%m%dT%H%M%S')
        tile_id = product_name.split('_')[3][1:]
        
        if json_inputs is None:
            program_path = shutil.which('ProcessRiverIce')
            program_dir_path = os.path.dirname(program_path)
                
            #template files
            template_dir_path = os.path.join(program_dir_path, 'metadata')
            template_files = {'rlie_s2': os.path.join(template_dir_path, 'RLIE_Metadata.xml'), \
                'rlie_s1': os.path.join(template_dir_path, 'RLIE_S1_Metadata.xml'), \
                'rlie_s1s2': os.path.join(template_dir_path, 'RLIE_S1S2_Metadata.xml'), \
                'arlie_s2': os.path.join(template_dir_path, 'ARLIE_Metadata.xml')}
            #general info
            general_info_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'templates', 'general_info.yaml')
            assert os.path.exists(general_info_file), 'could not find general info file on si_software install directory at %s'%general_info_file
            general_info = load_yaml(general_info_file, env_vars=False)
            assert general_info['product_version'] == product_name.split('_')[-2]
            json_inputs = {'RlieS1MetadataTemplatePath': template_files['rlie_s1'], \
                'HelpDeskEmail': general_info['helpdesk_email'], \
                'ProductVersion': general_info['product_version'], \
                'PumUrl': general_info['pum_url'], \
                'DiasUrl': general_info['dias_url'], \
                'DiasPortalName': general_info['dias_portal_name'], \
                'ValidationReportFilename': 'hrsi-ice-qar', \
                'ValidationReportDate': general_info['report_date']}
                
        
        print('Generating product %s :'%product_name)
        dem_data = read_band(dem_file_loc)
        raster_perim = RasterPerimeter(dem_file_loc)
        lonlat_minmax_dict = raster_perim.get_lonlat_minmax()
        latitude_loc = 0.5*(lonlat_minmax_dict['latmin']+lonlat_minmax_dict['latmax'])
        
        #RLIE file
        rlie_file_loc_temp = os.path.join(product_dir, '%s_RLIE.tif'%product_name)
        initialize_raster(raster_perim.info, rlie_file_loc_temp, nbands=1, dtype='u1', nodata_value=255, fill_all_value=255, random_dict=None, compress=False)
        cmd = "gdal_rasterize -burn 1 -l %s %s %s"%(os.path.basename(euhydro_shapefile)[:-4], euhydro_shapefile, rlie_file_loc_temp)
        subprocess.check_call(cmd, shell=True)
        
        #put ICE if temperature < 0°C with a modelled temperature
        #sun position from -23.5 lat in winter to 23.5 lat in summer : delay equinox by approx 1 month to take into account atmosphere inertia
        latitude_sun = -23.5+23.5*2*np.cos((acquisition_start_date-datetime(2010,7,21)).total_seconds()/(365.25*24.*3600.))
        #temperature = 45*cos(latitude-latitude_sun) - 1°C / 100m d+
        temperature = 45.*np.cos((latitude_loc-latitude_sun)*np.pi/180.) - dem_data/100.
        
        ds = gdal.Open(rlie_file_loc_temp, 1)
        band = ds.GetRasterBand(1)
        ar_in = np.ma.masked_invalid(band.ReadAsArray())
        ar_in.mask[ar_in == 255] = True
        ar_in[np.logical_and(~ar_in.mask, temperature<0.)] = 100
        band.WriteArray(ar_in)
        band.FlushCache()
        ds, band = None, None
        del ds, band
        
        
        #QC file
        qc_file_loc_temp = os.path.join(product_dir, '%s_QC.tif'%product_name)
        shutil.copy(rlie_file_loc_temp, qc_file_loc_temp)
        ds = gdal.Open(qc_file_loc_temp, 1)
        band = ds.GetRasterBand(1)
        ar_qc = np.ma.masked_invalid(np.random.randint(1, 5, size=np.prod(np.shape(ar_in)), dtype=ar_in.dtype).reshape(np.shape(ar_in)))
        ar_qc.mask[ar_in.mask] = True
        band.WriteArray(ar_in)
        band.FlushCache()
        ds, band = None, None
        del ds, band
        
        
        #QCFLAGS file
        qcflags_file_loc_temp = os.path.join(product_dir, '%s_QCFLAGS.tif'%product_name)
        initialize_raster(raster_perim.info, qcflags_file_loc_temp, nbands=1, dtype='u1', nodata_value=None, fill_all_value=0)
        
        
        #XML file
        with open(json_inputs['RlieS1MetadataTemplatePath']) as ds:
            txt_loc = ds.read()
        txt_loc = txt_loc.replace('[PRODUCT_ID]', product_name)
        txt_loc = txt_loc.replace('[HELPDESK_EMAIL]', json_inputs['HelpDeskEmail'])
        txt_loc = txt_loc.replace('[PRODUCTION_DATE]', datetime_now.strftime('%Y-%m-%dT%H:%M:%S.%f%Z'))
        txt_loc = txt_loc.replace('[ZONE_OF_SOURCE_TILE]', '')
        txt_loc = txt_loc.replace('[PRODUCT_VERSION]', json_inputs['ProductVersion'])
        txt_loc = txt_loc.replace('[EDITION_DATE]', datetime_now.strftime('%Y-%m-%dT%H:%M:%S.%f%Z'))
        txt_loc = txt_loc.replace('[PUM_URL]', json_inputs['PumUrl'])
        txt_loc = txt_loc.replace('[WB_lon]', '%s'%lonlat_minmax_dict['lonmin'])
        txt_loc = txt_loc.replace('[EB_lon]', '%s'%lonlat_minmax_dict['lonmax'])
        txt_loc = txt_loc.replace('[SB_lat]', '%s'%lonlat_minmax_dict['latmin'])
        txt_loc = txt_loc.replace('[NB_lat]', '%s'%lonlat_minmax_dict['latmax'])
        txt_loc = txt_loc.replace('[ACQUISITION_START]', acquisition_start_date.strftime('%Y-%m-%dT%H:%M:%S.%f%Z'))
        txt_loc = txt_loc.replace('[ACQUISITION_STOP]', (acquisition_start_date + timedelta(0,60)).strftime('%Y-%m-%dT%H:%M:%S.%f%Z'))
        txt_loc = txt_loc.replace('[DIAS_URL]', json_inputs['DiasUrl'])
        txt_loc = txt_loc.replace('[DIAS_PORTAL_NAME]', json_inputs['DiasPortalName'])
        txt_loc = txt_loc.replace('[VALIDATION_REPORT_FILENAME]', json_inputs['ValidationReportFilename'])
        txt_loc = txt_loc.replace('[REPORT_DATE]', json_inputs['ValidationReportDate'])
        with open(os.path.join(product_dir, '%s_MTD.xml'%product_name), mode='w') as ds:
            ds.write(txt_loc)
            
        
        if after_si_software_mode:
            if product_geometry is None:
                product_geometry = RasterPerimeter(dem_file_loc).projected_perimeter('epsg:4326')
            ice_s1_product_final_editing(product_dir, os.path.join(output_dir, product_name), product_geometry, apply_dem_mask_file=dem_file_loc)
            print('  -> generated product files (+json, cog and color edition) in %s'%os.path.join(output_dir, product_name))
        else:
            for el in os.listdir(product_dir):
                shutil.move(os.path.join(product_dir, el), os.path.join(output_dir, el))
            print('  -> generated product files in %s'%output_dir)
    
    finally:
        shutil.rmtree(temp_dir_session)
        
    
########################################
if __name__ == '__main__':
    
    try:
        import argparse
        parser = argparse.ArgumentParser(description='This script is used to generate simulated RLIE S1 products in the same format as those from ProcessRiverIce, on a specific S2 tile defined by DEM file.')
        parser.add_argument("--output_dir", type=str, required=True, help='path to output directory (will be created if missing, must be empty unless --overwrite option is used)')
        parser.add_argument("--product_name", type=str, required=True, help='RLIE S1 product name')
        parser.add_argument("--dem_path", type=str, required=True, help='path to DEM file dem_20m.tif for the requested S2 tile')
        parser.add_argument("--euhydro_shapefile", type=str, required=True, help='path to ASTRI eu_hydro_3035.shp shapefile')
        parser.add_argument("--json_inputs", type=str, help='JSON file containing template paths, ProductVersion, etc...')
        parser.add_argument("--temp_dir", type=str, help='path to temporary directory, current working directory by default')
        parser.add_argument("--after_si_software_mode", action='store_true', help='after_si_software_mode : organize in directories with product name and cog files')
        args = parser.parse_args()
        
    except Exception as ex:
        print(str(ex))
        sys.exit(exitcodes.wrong_input_parameters)
        
    if args.json_inputs is not None:
        with open(args.json_inputs) as ds:
            args.json_inputs = json.load(ds)
        
    #main function
    generate_simulated_rlie_s1_product_over_s2_tile(args.output_dir, args.product_name, args.dem_path, args.euhydro_shapefile, json_inputs=args.json_inputs, \
        after_si_software_mode=args.after_si_software_mode, temp_dir=args.temp_dir)

