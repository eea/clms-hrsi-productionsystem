#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_common.yaml_parser import load_yaml
from si_geometry.geometry_functions import *
import multiprocessing
from si_common.no_rlie_tiles import no_rlie_tiles
import yaml


def arlie_product_final_editing(arlie_product_input_dir, output_dir):
    product_id = '_'.join(os.listdir(arlie_product_input_dir)[0].split('_')[0:-1])    
    arlie_product_output_dir = os.path.join(output_dir, product_id)
    replicate_directory_with_compressed_tiffs(arlie_product_input_dir, output_fol=arlie_product_output_dir, add_prefix=product_id + '_', add_overviews=True)
    
    json_dict = {
        "collection_name": "HR-S&I",
        "resto": {
            "type": "Feature",
            "geometry": {
                "wkt": None
            },
            "properties": {
                "productIdentifier": None,
                "title": product_id,
                "resourceSize": compute_size_du(arlie_product_output_dir),
                "organisationName": "EEA",
                "startDate": None,
                "completionDate": None,
                "productType": "ARLIE",
                "processingBaseline": product_id.split('_')[-1],
                "host_base": None,
                "s3_bucket": None
            }}}
            
    with open('%s/dias_catalog_submit.json'%arlie_product_output_dir, mode='w') as ds:
        json.dump(json_dict, ds, ensure_ascii=True, indent=4)
    
    
    
def arlie_processing_basin(input_dir, output_dir, start_date, end_date, basin_name, arlie_aoi_dir, eu_hydro_basin_shapefile, s2_eea39_dict, temp_dir=None, nprocs=1):
    
    #load basin_arlie_aoi_shp_paths
    arlie_shapefiles = [el for el in os.listdir(arlie_aoi_dir) if '.shp' in el and basin_name in el]
    if len(arlie_shapefiles) > 1:
        print(arlie_shapefiles)
        raise Exception('found multiple match in %s for basin %s'%(arlie_aoi_dir, basin_name))
    elif len(arlie_shapefiles) < 1:
        print('Not processing basin %s due to lack of matching ARLIE shapefile'%basin_name)
        return
    arlie_aoi_shp_path = os.path.join(arlie_aoi_dir, arlie_shapefiles[0])
        
    print('Starting ARLIE processing for basin %s ...'%basin_name)
    
    print('  -> getting basin zone info')
    tstart = time.time()
    basin_shape = None
    with fiona.open(eu_hydro_basin_shapefile) as ds:
        for feature in ds:
            if feature['properties']['name'] != basin_name:
                continue
            if basin_shape is None:
                basin_shape = shape(feature['geometry'])
            else:
                raise Exception('basin %s encountered multiple times in %s'%(basin_name, eu_hydro_basin_shapefile))
    assert basin_shape is not None, 'basin %s not found in %s'%(basin_name, eu_hydro_basin_shapefile)
    basin_zone_info = get_basin_zone_info(basin_name, basin_shape, s2_eea39_dict)
    print('  -> basin zone info successfully read in %s seconds'%(time.time()-tstart))
    
    os.makedirs(temp_dir, exist_ok=True)
    temp_dir_session = tempfile.mkdtemp(prefix='temp_', dir=temp_dir)
    
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
    
    #metadata file
    arlie_metadata_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'templates', 'ARLIE_Metadata.xml')
    assert os.path.exists(arlie_metadata_file), 'ARLIE metadata file not found at %s'%arlie_metadata_file
        
    #appsettings.json file
    general_info_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'templates', 'general_info.yaml')
    assert os.path.exists(general_info_file), 'general info file not found at %s'%general_info_file
    with open(general_info_file) as ds:
        general_info_template = yaml.load(ds)
    dico_json = {"Configuration" : {"SnapPath": shutil.which('gpt'), \
            "GraphPath": graph_file, \
            "ARlieMetadataTemplatePath": arlie_metadata_file, \
            "MaxThreads": nprocs, \
            "HelpDeskEmail": general_info_template['helpdesk_email'], \
            "ProductVersion": general_info_template['product_version'], \
            "PumUrl": general_info_template['pum_url'], \
            "DiasUrl": general_info_template['dias_url'], \
            "DiasPortalName": general_info_template['dias_portal_name'], \
            "ValidationReportFilename": 'hrsi-ice-qar', \
            "ValidationReportDate": general_info_template['report_date']}}
    dico_json['Serilog'] = {'MinimumLevel': 'Debug', 'WriteTo': [{'Name': 'Async', 'Args': {'configure': [{'Name': 'Console', \
        "outputTemplate": "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level}] [{SourceContext}] [{EventId}] {Message}{NewLine}{Exception}"}]}}]}
    with open('%s/appsettings.json'%temp_dir_session, mode='w') as ds:
        json.dump(dico_json, ds)
        
    #make tile list file
    with open('%s/%s.txt'%(temp_dir_session, basin_name), mode='w') as ds:
        ds.write('BoundingBox:\n%.7f,%.7f\n%.7f,%.7f\nTiles:\n%s\n'%(basin_zone_info['lonmin'], basin_zone_info['latmin'], \
            basin_zone_info['lonmax'], basin_zone_info['latmax'], '\n'.join(basin_zone_info['s2_tile_list'])))

    cmd = [program_path, 'ARLIE', start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), input_dir, arlie_aoi_shp_path, '%s/%s.txt'%(temp_dir_session, basin_name), \
        '%s/out'%temp_dir_session, '%s/appsettings.json'%temp_dir_session]
    print(' '.join(cmd))
    try:
        subprocess.check_call(cmd)
        arlie_product_final_editing('%s/out'%temp_dir_session, output_dir)
        print('  -> ARLIE processing for basin %s succeeded !'%basin_name)
        return True
    except:
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        print('  -> ARLIE processing for basin %s failed !'%basin_name)
        return False
    finally:
        shutil.rmtree(temp_dir_session)
    
    
def get_basin_zone_info(basin_name, geom, s2_eea39_dict):
    print('  -> computing tiles intersecting with basin %s'%basin_name)
    laea_prj = 'PROJCS["ETRS89_LAEA_Europe",GEOGCS["GCS_ETRS_1989",DATUM["D_ETRS_1989",SPHEROID["GRS_1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Lambert_Azimuthal_Equal_Area"],PARAMETER["latitude_of_origin",52],PARAMETER["central_meridian",10],PARAMETER["false_easting",4321000],PARAMETER["false_northing",3210000],UNIT["Meter",1]]'
    dico = {'basin_name': basin_name, 's2_tile_list': []}
    geom_latlon = project_polygon_to_different_coordinate_system(geom, laea_prj, 'epsg:4326', npoints_per_edge=10)
    dico['latmin'], dico['lonmin'], dico['latmax'], dico['lonmax'] = geom_latlon.bounds

    for tile_id in s2_eea39_dict:
        if geom.intersects(RasterPerimeter(s2_eea39_dict[tile_id]).projected_perimeter(laea_prj, npoints_per_edge=10)):
            dico['s2_tile_list'].append(tile_id)
    return dico
    
    
def arlie_processing_chain(output_dir, temp_dir, arlie_aoi_dir, aoi_eea39_dir, start_date, end_date, input_rlie_dir, nprocs=1, sequential=True):
    
    assert 1 <= nprocs <= multiprocessing.cpu_count(), 'nprocs (%d) must be >=1 and <= multiprocessing.cpu_count() = %d in this instance.'%(nprocs, multiprocessing.cpu_count())
    
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    
    print('Reading metadata')
    s2_eea39_dict = json.load(open(os.path.join(aoi_eea39_dir, 's2tiles_eea39', 's2tiles_eea39_gdal_info.json')))
    s2_eea39_dict = {key: val for key, val in s2_eea39_dict.items() if key not in no_rlie_tiles}
    
    #load basin polygons, reprojected to 'epsg:4326'
    eu_hydro_basin_shapefile = os.path.join(aoi_eea39_dir, 'eu_hydro_merged_shapefiles', 'eu_hydro_riverbasins.shp')
    assert os.path.exists(eu_hydro_basin_shapefile), 'basin shapefile not found at %s'%eu_hydro_basin_shapefile
    with fiona.open(eu_hydro_basin_shapefile) as ds:
        basin_names = [feature['properties']['name'] for feature in ds]
        
    
    ###############
    #get rlie products and put symbolic links in an input dir
    rlie_products = search_folder_structure(input_rlie_dir, regexp='RLIE_*', maxdepth=5, object_type='d', case_sensible=True)
    rlie_product_dict = {tile_id: [] for tile_id in sorted(list(set(s2_eea39_dict.keys())))}
    for rlie_product in rlie_products:
        measurement_date = datetime.strptime(os.path.basename(rlie_product).split('_')[1], '%Y%m%dT%H%M%S')
        if (measurement_date > end_date) or (measurement_date < start_date):
            continue
        tile_id = os.path.basename(rlie_product).split('_')[3][1:]
        if tile_id not in rlie_product_dict:
            continue
        rlie_product_dict[tile_id].append(rlie_product)
            
    #put symbolic links in an input dir
    input_dir = tempfile.mkdtemp(dir=temp_dir)
    for tile_id in rlie_product_dict:
        for product_path in rlie_product_dict[tile_id]:
            os.symlink(product_path, os.path.join(input_dir, os.path.basename(product_path)))
    ################        
            
            
    #launch ARLIE calculations
    if sequential:
        #run ARLIE processing for each basin sequentially
        for basin_name in basin_names:
            success = arlie_processing_basin(input_dir, os.path.join(output_dir, basin_name), start_date, end_date, basin_name, arlie_aoi_dir, eu_hydro_basin_shapefile, s2_eea39_dict, \
                temp_dir=os.path.join(temp_dir, basin_name), nprocs=nprocs)
            if not success:
                raise Exception('basin %s unsuccessful'%basin_name)
    else:
        #run ARLIE processing for each basin in parallel
        pool = multiprocessing.Pool(processes=nprocs)
        results = pool.starmap(arlie_processing_basin, [(input_dir, os.path.join(output_dir, basin_name), start_date, end_date, basin_name, arlie_aoi_dir, eu_hydro_basin_shapefile, \
            s2_eea39_dict, os.path.join(temp_dir, basin_name), 1) for basin_name in basin_names])
        basins_failed = [basin_name for ii, basin_name in enumerate(basin_names) if not results[ii]]
        if len(basins_failed) > 0:
            raise Exception('the following basins failed to generate a proper product:\n%s\n'%('\n'.join(['- %s'%basin_name for basin_name in basins_failed])))
            





if __name__ == '__main__':
    

    import argparse
    parser = argparse.ArgumentParser(description='This script is used to launch ARLIE product generation for all EEA39 tiles')
    parser.add_argument("--output_dir", type=str, required=True, help='output directory')
    parser.add_argument("--temp_dir", type=str, help='temp directory')
    
    parser.add_argument("--trimester", type=str, help='trimester to compute ARLIE. Example : 2019,1 will compute between 2019/01/01 and 2019/04/01.')
    parser.add_argument("--start_date", type=str, help='start_date to compute ARLIE in Y-m-d format')
    parser.add_argument("--end_date", type=str, help='end_date to compute ARLIE in Y-m-d format. Warning: since the computation ends at Y-m-dT00:00:00, the end_date day is not included in the calculation.')

    parser.add_argument("--nprocs", type=int, default=4, help='number of procs to use, default is 4')
    parser.add_argument("--sequential", action='store_true', help='process basin per basin in parallel instead of basins in parallel on 1 proc each.')
    parser.add_argument("--max_ram", type=int, default=4096, help='mac ram to use for gdal/OTB, default is 4096')
    
    parser.add_argument("--input_rlie_dir", type=str, help='input RLIE directory containing tile_id/year/month/day/products or year/month/day/products subfolder structure')
    parser.add_argument("--arlie_aoi_dir", type=str, help='path to arlie_aoi_dir')
    parser.add_argument("--aoi_eea39_dir", type=str, help='path to aoi_eea39_dir')
    parser.add_argument("--on_cnes_hpc", action='store_true', help="CNES reprocessing mode : completes some options by default")
    args = parser.parse_args()
    
    #dates
    if args.trimester is None:
        assert args.start_date is not None
        assert args.end_date is not None
        args.start_date = datetime.strptime(args.start_date + 'T000000', '%Y-%m-%dT%H%M%S')
        args.end_date = datetime.strptime(args.end_date + 'T000000', '%Y-%m-%dT%H%M%S')
    else:
        assert args.start_date is None
        assert args.end_date is None
        args.trimester = [int(el) for el in args.trimester.split(',')]
        assert len(args.trimester) == 2
        assert args.trimester[1] in range(1,5)
        args.start_date = datetime(args.trimester[0],1+(args.trimester[1]-1)*3,1)
        if args.trimester[1] < 4:
            args.end_date = datetime(args.trimester[0],1+args.trimester[1]*3,1)
        else:
            args.end_date = datetime(args.trimester[0]+1,1,1)
    print('Start date: %s'%args.start_date)
    print('End date: %s'%args.end_date)

    #temp dir
    if args.temp_dir is None:
        assert 'TMPDIR' in os.environ
        args.temp_dir = os.environ['TMPDIR']
    
    #cnes HPC set values  
    if args.on_cnes_hpc:
        from si_reprocessing import install_cnes
        if args.arlie_aoi_dir is None:
            args.arlie_aoi_dir = os.path.join(install_cnes.reprocessing_static_data_dir, 'hidden_value', 'ARLIE_AOI')
        if args.aoi_eea39_dir is None:
            args.aoi_eea39_dir = os.path.join(install_cnes.reprocessing_static_data_dir, 'hidden_value', 'AOI_EEA39')
        if (args.input_rlie_dir is None):
            args.input_rlie_dir = os.path.join(install_cnes.reprocessing_cnes_hrsi_storage_dir, 'Ice', 'RLIE')
    else:
        assert args.aoi_eea39_dir is not None, 'aoi_eea39_dir must be defined'
        

    arlie_processing_chain(args.output_dir, args.temp_dir, args.arlie_aoi_dir, args.aoi_eea39_dir, args.start_date, args.end_date, args.input_rlie_dir, \
        nprocs=args.nprocs, sequential=args.sequential)
