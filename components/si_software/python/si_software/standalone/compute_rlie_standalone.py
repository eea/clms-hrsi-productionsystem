#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_common.follow_process import execute_commands, dump_execution_dict_to_directory
from si_common.yaml_parser import load_yaml, dump_yaml

from si_geometry.geometry_functions import *

from si_utils.rewrite_cog import rewrite_cog

from si_software.add_colortable_to_si_products import add_colortable_to_si_products
from si_software.add_quicklook import add_quicklook
from si_software.maja_l2a_processing import maja_l2a_processing, update_product_information_from_l2a_file


def get_latitude_tag(value):
    assert isinstance(value, int)
    assert -90 <= value <= 90
    if value < 0:
        return '%dS'%(-value)
    else:
        return '%dN'%(value)

        

def get_longitude_tag(value):
    assert isinstance(value, int)
    val = value%360
    if val > 180:
        val -= 360
    if val < 0:
        return '%dW'%(-val)
    else:
        return '%dE'%(val)




def ice_product_final_editing(ice_product_input_dir, ice_product_output_dir, product_information, apply_dem_mask_file=None):
    
    files_produced = os.listdir(ice_product_input_dir)
    rlie_file = [el for el in files_produced if 'RLIE.tif' in el]
    assert len(rlie_file) == 1
    rlie_file = rlie_file[0]
    product_id = '_'.join(rlie_file.split('_')[0:-1])
    files_checked = []
    for expected_sufix in ['RLIE.tif', 'QC.tif', 'QCFLAGS.tif', 'MTD.xml']:
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
    add_colortable_to_si_products(input_product_tagged_dir, product_tag=product_id)
    
    #transform geotiff into COG
    rewrite_cog(input_product_tagged_dir, dest_path=ice_product_output_dir, verbose=1)
    
    #add quicklook
    add_quicklook(ice_product_output_dir, '_RLIE.tif')
    
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
    
    
    
def make_ice_script(river_shapefile, hrl_flags_file, ice_temp_dir, l2a_file, rlie_metadata_file, product_information, nprocs_ice):
    
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
    graph_file = os.path.join(program_dir_path, 'graphs', 'preprocessing_classification_fre_5class_md20m_1input_14flat_bsi_cut_auto.xml')
    assert os.path.exists(graph_file), 'graph file not found: %s'%graph_file
        
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
        
    cmd = ['ProcessRiverIce', 'RLIE', l2a_xml, river_shapefile, hrl_flags_file, '%s/out'%ice_temp_dir, '%s/appsettings.json'%ice_temp_dir]
    return cmd
    
    

def raster_to_shapefile(in_file, out_file):
    cmdline = ['gdal_polygonize.py', '-8', in_file, out_file]
    subprocess.call(cmdline)
    
    
def get_gsw_tile_dict():
    main_url = 'https://storage.googleapis.com/global-surface-water/downloads2020/occurrence'
    main_filename = 'occurrence_%s_%sv1_3_2020.tif'
    
    dico = dict()
    for lonmin in range(-180, 180, 10):
        for latmax in range(-80, 90+10, 10):
            lon_tag = get_longitude_tag(lonmin)
            lat_tag = get_latitude_tag(latmax)
            dico['%d_%d'%(lonmin, latmax)] = {'shape': Polygon([(lonmin, latmax-10), (lonmin+10, latmax-10), (lonmin+10, latmax), (lonmin, latmax)])}
            if (latmax >= -50) and (latmax <= 80):
                dico['%d_%d'%(lonmin, latmax)]['valid'] = True
                dico['%d_%d'%(lonmin, latmax)]['url'] = os.path.join(main_url, main_filename%(lon_tag, lat_tag))
                dico['%d_%d'%(lonmin, latmax)]['filename'] = main_filename%(lon_tag, lat_tag)
            else:
                dico['%d_%d'%(lonmin, latmax)]['valid'] = False
                
    return dico
    
    
def download_gsw_file(url, gsw_dir, temp_dir=None):
    
    #handle temp dir
    if temp_dir is not None:
        os.makedirs(temp_dir, exist_ok=True)
    temp_dir_session = tempfile.mkdtemp(dir=temp_dir, prefix='dlgsw_')
    
    try:
        
        target_path = os.path.join(gsw_dir, os.path.basename(url))
        if not os.path.exists(target_path):
            cwd = os.getcwd()
            os.chdir(temp_dir_session)
            os.system('wget %s'%url)
            os.chdir(cwd)
            filepath = os.path.join(temp_dir_session, os.path.basename(url))
            assert os.path.exists(filepath), 'download not successful for %s'%url
        if not os.path.exists(target_path):
            shutil.move(filepath, os.path.join(gsw_dir, os.path.basename(url)))
        
    finally:
    
        #remove temp dir
        shutil.rmtree(temp_dir_session)
        
    
def make_water_shapefile_from_gsw(river_shapefile, model_gdal_info, gsw_dir=None, gsw_threshold=None, temp_dir=None):
    
    if gsw_threshold is None:
        gsw_threshold = 90
    assert isinstance(gsw_threshold, int)
    
    #handle temp dir
    if temp_dir is not None:
        os.makedirs(temp_dir, exist_ok=True)
    temp_dir_session = tempfile.mkdtemp(dir=temp_dir, prefix='shpfromgsw_')
    
    try:
        
        #init GSW dir
        if gsw_dir is None:
            gsw_dir = os.path.join(temp_dir_session, 'gsw_temp_storage')
        os.makedirs(gsw_dir, exist_ok=True)
        
        #compute GSW borders
        tile_borders = RasterPerimeter(model_gdal_info).projected_perimeter('epsg:4326')
        
        #get GSW files, donwload if required
        gsw_files_required = []
        dico_gsw = get_gsw_tile_dict()
        for tag, dico_gsw_loc in dico_gsw.items():
            if tile_borders.intersects(dico_gsw_loc['shape']):
                if not dico_gsw_loc['valid']:
                    raise Exception('target tile spans outside GSW area of definition, tried to get GSW %s'%tag)
                gsw_path_loc = os.path.join(gsw_dir, dico_gsw_loc['filename'])
                if not os.path.exists(gsw_path_loc):
                    print('Downloading %s'%dico_gsw_loc['url'])
                    download_gsw_file(dico_gsw_loc['url'], gsw_dir, temp_dir=temp_dir_session)
                gsw_files_required.append(gsw_path_loc)

        #build VRT if necessary (multiple files)
        if len(gsw_files_required) == 0:
            raise Exception('No intersection with earth, weird...')
        elif len(gsw_files_required) == 1:
            vrt_file = gsw_files_required[0]
        else:
            vrt_file = os.path.join(temp_dir_session, 'gsw_occurrence.vrt')
            subprocess.check_call(['gdalbuildvrt', vrt_file] + gsw_files_required)
            
        #project GSW occurrence to target raster
        target_raster_occurrence = os.path.join(temp_dir_session, 'gsw_occurrence.tif')
        gdal.Warp(target_raster_occurrence, vrt_file, options=gdal.WarpOptions(format='GTiff', \
            outputBounds=tuple(model_gdal_info['cornerCoordinates']['lowerLeft'] + model_gdal_info['cornerCoordinates']['upperRight']), \
            xRes=int(abs(model_gdal_info['geoTransform'][1])), yRes=int(abs(model_gdal_info['geoTransform'][-1])), \
            dstSRS=model_gdal_info['coordinateSystem']['wkt'], outputType=gdal.GDT_Byte, warpMemoryLimit=4000., resampleAlg='near', \
            creationOptions=['compress=deflate', 'zlevel=4']))
            
        #select water extent based on gsw_threshold on occurrence
        target_raster_extent = os.path.join(temp_dir_session, 'gsw_extent.tif')
        ar0 = initialize_raster(model_gdal_info, target_raster_extent)
        ds = gdal.Open(target_raster_occurrence)
        band = ds.GetRasterBand(1)
        ar_occurrence = np.ma.masked_invalid(band.ReadAsArray())
        no_data_value = band.GetNoDataValue()
        ar_occurrence.mask[ar_occurrence == no_data_value] = True
        band, ds = None, None
        del band, ds
        
        ar0[ar_occurrence >= gsw_threshold] = 1
        ar0[ar_occurrence.mask] = 0
        
        ds = gdal.Open(target_raster_extent, 1)
        band = ds.GetRasterBand(1)
        band.SetNoDataValue(0.)
        band.WriteArray(ar0)
        band.FlushCache()
        band, ds = None, None
        del band, ds
        compress_geotiff_file(target_raster_extent)
        
        #create shapefile from extent raster
        river_shapefile_temp = os.path.join(temp_dir_session, 'gsw_extent.shp')
        subprocess.check_call(['gdal_polygonize.py', '-8', target_raster_extent, river_shapefile])
        with open(river_shapefile.replace('.shp', '.prj'), mode='w') as ds:
            ds.write(proj_to_prj(model_gdal_info['coordinateSystem']['wkt']))
        # ~ project_polygon_shapefile_to_different_coordinate_system(river_shapefile_temp, river_shapefile, 'epsg:4326', proj_in_str=model_gdal_info['coordinateSystem']['wkt'], npoints_per_edge=10)
        shutil.move(target_raster_extent, river_shapefile.replace('.shp', '.tif'))
        
    finally:
    
        #remove temp dir
        shutil.rmtree(temp_dir_session)
    
    
    
def compute_rlie(l2a_file, output_dir, hydro_shape_src=None, eu_hydro_dir=None, gsw_dir=None, gsw_threshold=None, specific_file=None, \
        hrl_dir=None, temp_dir=None, nprocs=1):
            
    #select L2A model file (B3) and check that all necessary bands exist
    l2a_tag = os.path.basename(l2a_file)
    tile_id = l2a_tag.split('_')[-3][1:]
    model_l2a_tif = os.path.join(l2a_file, l2a_tag + '_FRE_B3.tif')
    model_gdal_info = gdal.Info(model_l2a_tif, format='json')
    model_gdal_info_div2 = gdal.Info(os.path.join(l2a_file, l2a_tag + '_FRE_B12.tif'), format='json')
    for tag in sorted(list(set(['B%d'%el for el in range(2,12+1)]) - set(['B1', 'B9', 'B10'])) + ['B8A']):
        filepath_loc = os.path.join(l2a_file, l2a_tag + '_FRE_%s.tif'%tag)
        assert os.path.exists(filepath_loc), 'file %s missing from L2A'%filepath_loc
    
    #handle temp dir
    if temp_dir is not None:
        os.makedirs(temp_dir, exist_ok=True)
    temp_dir_session = tempfile.mkdtemp(dir=temp_dir, prefix='computerlie_')
    
    try:
    
        #create output dir
        os.makedirs(output_dir, exist_ok=True)

        #load necessary info / make up empty product information
        template_file = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'templates', 'RLIE_Metadata.xml')
        
        #product information
        product_information = dict()
        general_info_file = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'templates', 'general_info.yaml')
        product_information['general_info'] = load_yaml(general_info_file, env_vars=False)
        product_information['product_mode_overide'] = 0
        product_information['production_date'] = datetime.utcnow()
        product_information = update_product_information_from_l2a_file(product_information, l2a_file, temp_dir=temp_dir_session)
        
        #handle shapefiles
        if hydro_shape_src is None:
            hydro_shape_src = 'eu_hydro_gsw'
            
        #select eu hydro and then gsw if eu hydro does not exist
        if hydro_shape_src == 'eu_hydro_gsw':
            assert eu_hydro_dir is not None
            river_shapefile = os.path.join(eu_hydro_dir, tile_id, 'eu_hydro_%s.shp'%tile_id)
            if os.path.exists(river_shapefile):
                hydro_shape_src = 'eu_hydro'
            else:
                hydro_shape_src = 'gsw'
        
        #hydrological zone selection
        if hydro_shape_src == 'eu_hydro':
            assert eu_hydro_dir is not None
            river_shapefile = os.path.join(eu_hydro_dir, tile_id, 'eu_hydro_%s.shp'%tile_id)
            assert os.path.exists(river_shapefile), 'Eu-HYDRO shapefile %s not found'%river_shapefile
        elif hydro_shape_src == 'full_tile':
            river_shapefile = os.path.join(temp_dir_session, 'hydro_%s.shp'%tile_id)
            RasterPerimeter(model_gdal_info).to_epsg4326_shapefile(river_shapefile)
        elif hydro_shape_src == 'gsw':
            river_shapefile = os.path.join(temp_dir_session, 'hydro_%s.shp'%tile_id)
            make_water_shapefile_from_gsw(river_shapefile, model_gdal_info, gsw_dir=gsw_dir, gsw_threshold=gsw_threshold, temp_dir=temp_dir_session)
        elif hydro_shape_src == 'specific_file':
            assert specific_file is not None
            assert os.path.exists(specific_file), 'specific file %s not found'%specific_file
            if specific_file.split('.')[-1] == 'shp':
                river_shapefile = specific_file
            elif specific_file.split('.')[-1] == 'tif':
                river_shapefile = os.path.join(temp_dir_session, 'hydro_%s.shp'%tile_id)
                raster_to_shapefile(specific_file, river_shapefile)
            else:
                raise Exception('specific file format should be tif or shp.')
        else:
            raise Exception('unknown hydro_shape_src: %s'%hydro_shape_src)

        
        #handle HRL flags file
        hrl_file = None
        if hrl_dir is not None:
            hrl_file_loc = os.path.join(hrl_dir, tile_id, 'hrl_qc_flags_%s.tif'%tile_id)
            if os.path.exists(hrl_file_loc):
                hrl_file = hrl_file_loc
        if hrl_file is None:
            hrl_file = os.path.join(temp_dir_session, 'hrl_qc_flags_%s.tif'%tile_id)
            initialize_raster(model_gdal_info_div2, hrl_file, nbands=1, dtype='u1', nodata_value=0, fill_all_value=0, compress=True)
        
        ######################
        #execute ICE
        cmd_ice = make_ice_script(river_shapefile, hrl_file, temp_dir_session, l2a_file, template_file, product_information, nprocs)
        print(' '.join(cmd_ice))
        os.system(' '.join(cmd_ice))
        
        rlie_products = os.listdir(os.path.join(temp_dir_session, 'out'))
        rlie_main_product = [el for el in rlie_products if '_RLIE.tif' in el]
        assert len(rlie_main_product) == 1
        rlie_main_product = rlie_main_product[0]
        rlie_product_tag = rlie_main_product.replace('_RLIE.tif','')
        if os.path.exists(os.path.join(output_dir, rlie_product_tag)):
            shutil.rmtree(os.path.join(output_dir, rlie_product_tag))
        ice_product_final_editing(os.path.join(temp_dir_session, 'out'), os.path.join(output_dir, rlie_product_tag), product_information, apply_dem_mask_file=None)
        shutil.copy(hrl_file, os.path.join(output_dir, rlie_product_tag, os.path.basename(hrl_file)))
        os.system('cp %s* %s/'%(river_shapefile.replace('.shp',''), os.path.join(output_dir, rlie_product_tag)))
        
    finally:
    
        #remove temp dir
        shutil.rmtree(temp_dir_session)




if __name__ == '__main__':
            
    import argparse
    parser = argparse.ArgumentParser(description="compute RLIE standalone", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--l2a", type=str, required=True, help="source L2A")
    parser.add_argument("--output_dir", type=str, required=True, help="output directory")
    parser.add_argument("--hydro_shape_src", type=str, choices=['eu_hydro', 'gsw', 'full_tile', 'specific_binary_raster', 'eu_hydro_gsw'], default='eu_hydro_gsw', help="Hydrological shape source. RLIE will be computed only within those shapes.")
    parser.add_argument("--eu_hydro_dir", type=str, help="Hydrological network shapefile directory with ${tile_id}/eu_hydro_${tile_id}.shp files. Used if --hydro_src='eu_hydro'.")
    parser.add_argument("--gsw_dir", type=str, help="Directory where GSW files are stored. Required GSW files will be downloaded. Used if --hydro_src='gsw'.")
    parser.add_argument("--gsw_threshold", type=int, default=90, help="Threshold : AOI is >= threshold value.")
    parser.add_argument("--specific_file", type=str, help="specific_file, either shapefile or raster (fitted to 20m L2A band for the latter).")
    parser.add_argument("--hrl_dir", type=str, help="HRL storage dir with ${tile_id}/hrl_qc_flags_${tile_id}.tif files. If not found, a full NAN HRL tif will be generated as a temp file and used.")
    parser.add_argument("--temp_dir", type=str, help="temp directory")
    parser.add_argument("--nprocs", help="nprocs", type=int)
    args = parser.parse_args()
    

    compute_rlie(args.l2a, args.output_dir, hydro_shape_src=args.hydro_shape_src, eu_hydro_dir=args.eu_hydro_dir, gsw_dir=args.gsw_dir, gsw_threshold=args.gsw_threshold, \
        specific_file=args.specific_file, hrl_dir=args.hrl_dir, temp_dir=args.temp_dir, nprocs=args.nprocs)
    
    

