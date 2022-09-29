#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_common.no_rlie_tiles import no_rlie_tiles
from si_geometry.geometry_functions import *
from shapely import wkt
from si_geometry.get_valid_data_convex_hull import get_valid_data_convex_hull


def get_dem_20m_paths(dem_dir, tile_ids):
    #dem files (file structure can be compatible with part 1 or part 2)
    has_part1_structure_dem_files = False
    dem_files_dict = dict()
    missing_dem_tiles = set()
    for tile_id in tile_ids:
        #dem file obeying part 2 dem_20m.tif file structure
        dem_path_part2 = os.path.join(dem_dir, 'dem_20m_%s.tif'%tile_id)
        #dem file obeying part 1 {0}/S2__TEST_AUX_REFDE2_T{0}_0001/S2__TEST_AUX_REFDE2_T{0}_0001.DBL.DIR/dem_20m.tif file structure
        dem_path_part1 = os.path.join(dem_dir, '{0}/S2__TEST_AUX_REFDE2_T{0}_0001/S2__TEST_AUX_REFDE2_T{0}_0001.DBL.DIR/dem_20m.tif'.format(tile_id))
        
        if os.path.exists(dem_path_part2):
            dem_files_dict[tile_id] = dem_path_part2
        elif os.path.exists(dem_path_part1):
            dem_files_dict[tile_id] = dem_path_part1
            has_part1_structure_dem_files = True
        else:
            missing_dem_tiles.add(tile_id)
    return dem_files_dict, missing_dem_tiles, has_part1_structure_dem_files


def get_s1_product_info(s1grd_path):
    s1grd_name = os.path.basename(s1grd_path)
    assert len(s1grd_name.split('_')) == 9
    satellite = s1grd_name.split('_')[0]
    satmode = s1grd_name.split('_')[1]
    acquisition_start_date = datetime.strptime(s1grd_name.split('_')[4], '%Y%m%dT%H%M%S')
    acquisition_stop_date = datetime.strptime(s1grd_name.split('_')[5], '%Y%m%dT%H%M%S')
    return {'satellite': satellite, 'satmode': satmode, 'acquisition_start_date': acquisition_start_date, 'acquisition_stop_date': acquisition_stop_date}
    
    
    
def get_vv_vh_files_from_s1_product(s1grd_path):
    assert os.path.exists(os.path.join(s1grd_path, 'measurement'))
    spl = os.path.basename(s1grd_path).split('_')
    assert spl[2] in ['GRD', 'GRDH']
    spl[2] = 'GRD'
    spl[3] = 'vv'
    spl[-1] = '001.tiff'
    vvfile = os.path.join(s1grd_path, 'measurement', '-'.join(spl).lower())
    assert os.path.exists(vvfile)
    spl[3] = 'vh'
    spl[-1] = '002.tiff'
    vhfile = os.path.join(s1grd_path, 'measurement', '-'.join(spl).lower())
    assert os.path.exists(vhfile)
    return vvfile, vhfile
    
    
def get_s1grd_perimeter(s1grd_product_path, npoints_per_edge=5):
    """returns lon,lat coordinates of bounding box in high resolution to accurately take into account the difference in coordinate system
    
    :param s1grd_product_path: path to s1grd_product"""
    
    import rasterio
    #NB: doesnt work because XML lon, lat box is actually not accurate 
    
    #get bounding box in lat lon from XML file
    manifest_file = os.path.join(s1grd_product_path, 'manifest.safe')
    with open(manifest_file) as ds:
        txt = ds.read()
    coors_txt = txt.split('<gml:coordinates>')[-1].split('</gml:coordinates>')[0].split(' ')
    assert len(coors_txt) == 4
    box_lat = np.array([float(el.split(',')[0]) for el in coors_txt], dtype=np.float64)
    box_lon = np.array([float(el.split(',')[1]) for el in coors_txt], dtype=np.float64)

    #get GCPs
    vvfile, _ = get_vv_vh_files_from_s1_product(s1grd_product_path)
    with rasterio.open(vvfile) as ds:
        assert hasattr(ds, 'gcps')
        assert len(ds.gcps[0]) > 0
        assert ds.gcps[1].to_epsg() == 4326
        transform_loc = rasterio.transform.from_gcps(ds.gcps[0])
    
    
    #get bounding box in row, col
    box_row, box_col = rasterio.transform.rowcol(transform_loc, box_lat, box_lon)
    #add first point to make complete polygon
    box_row.append(box_row[0])
    box_col.append(box_col[0])
    
    #add n points to each polygon edge (so that change in coor system still fits the borders accurately)
    dl = 1./npoints_per_edge
    box_row_new, box_col_new = [], []
    for i0 in range(len(box_row)-1):
        box_row_new += [box_row[i0]+(box_row[i0+1]-box_row[i0])*ii*dl for ii in range(npoints_per_edge)]
        box_col_new += [box_col[i0]+(box_col[i0+1]-box_col[i0])*ii*dl for ii in range(npoints_per_edge)]
    box_row_new = np.array(box_row_new, dtype=np.int32)
    box_col_new = np.array(box_col_new, dtype=np.int32)
    
    #convert to EPSG:4326
    lat, lon = rasterio.transform.xy(transform_loc, box_row_new, box_col_new)
    polygon_area_definition_s1grd = Polygon(zip(lon, lat))
    return polygon_area_definition_s1grd
    


def compute_intersecting_tile_ids_and_geometries(s1grd_path, s2tiles_eea39_gdal_info, output_file=None, temp_dir=None):
            
    #get s1 valid area geometry
    vvfile, _ = get_vv_vh_files_from_s1_product(s1grd_path)
    polygon_s1_valid = get_valid_data_convex_hull(vvfile, invalid_values=[0,1,2,3,4,5], proj_out='EPSG:4326', temp_dir=temp_dir, npoints_per_edge=2, alpha=0.5)
    # ~ try:
        # ~ polygon_s1_valid = get_valid_data_convex_hull(vvfile, invalid_values=[0], proj_out='EPSG:4326', temp_dir=temp_dir, npoints_per_edge=2, alpha=0.5)
    # ~ except:
        # ~ polygon_s1_valid = None
    # ~ if polygon_s1_valid is None:
        # ~ print('Failed to get S1 product exact alpha shape, trying to get a simpler shape by masking out values up to (and including) 4.')
        # ~ try:
            # ~ polygon_s1_valid = get_valid_data_convex_hull(vvfile, invalid_values=[0,1,2,3,4], proj_out='EPSG:4326', temp_dir=temp_dir, npoints_per_edge=2, alpha=0.5)
        # ~ except:
            # ~ polygon_s1_valid = None
    # ~ if polygon_s1_valid is None:
        # ~ print('Failed to get S1 product alpha shape again, trying to get an even simpler shape by masking out values up to (and including) 9.')
        # ~ polygon_s1_valid = get_valid_data_convex_hull(vvfile, invalid_values=[0,1,2,3,4,5,6,7,8,9], proj_out='EPSG:4326', temp_dir=temp_dir, npoints_per_edge=2, alpha=0.5)
            
            
    # ~ polygon_s1_valid = get_s1grd_perimeter(s1grd_path, npoints_per_edge=20)
    
    #iterate over S2 tiles and compute intersecting geometry for those that intersect with S1 tile
    dico = dict()
    with open(s2tiles_eea39_gdal_info) as ds:
        tile_dict = json.load(ds)
        
    for tile_id_loc, info_loc in tile_dict.items():
        if tile_id_loc in no_rlie_tiles:
            continue
        geom_loc = RasterPerimeter(info_loc).projected_perimeter('epsg:4326', npoints_per_edge=5)
        if polygon_s1_valid.intersects(geom_loc):
            dico[tile_id_loc] = polygon_s1_valid.intersection(geom_loc)
    
    if output_file is not None:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, mode='w') as ds:
            json.dump({tile_id: geom.wkt for tile_id, geom in dico.items()}, ds, indent=4)
            
        ########### remove later
        shapefile_out = output_file.replace('.json', '_shapefile_s1grd.shp')
        with fiona.open(shapefile_out, 'w', 'ESRI Shapefile', {'geometry': 'Polygon'}) as ds:
            ds.write({'geometry': mapping(polygon_s1_valid)})
        with open(shapefile_out.replace('.shp', '.prj'), mode='w') as ds:
            ds.write(CRS.from_user_input('epsg:4326').to_wkt())
        ###########
            
        ########### remove later
        shapefile_out = output_file.replace('.json', '_shapefile.shp')
        with fiona.open(shapefile_out, 'w', 'ESRI Shapefile', {'geometry': 'Polygon', 'properties': {'tile_id': 'str'}}) as ds:
            for tile_id, geom in dico.items():
                ds.write({'geometry': mapping(geom), 'properties': {'tile_id': tile_id}})
        with open(shapefile_out.replace('.shp', '.prj'), mode='w') as ds:
            ds.write(CRS.from_user_input('epsg:4326').to_wkt())
        ###########
    
    return dico
    


def load_intersecting_tile_ids_and_geometries(input_json_file):
    with open(input_json_file) as ds:
        dico = json.load(ds)
    for key in dico:
        dico[key] = wkt.loads(dico[key])
    return dico
    
    
