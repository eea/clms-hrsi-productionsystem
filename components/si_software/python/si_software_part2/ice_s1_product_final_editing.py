#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_geometry.geometry_functions import *
from si_software.add_colortable_to_si_products import add_colortable_to_si_products
from si_software.add_quicklook import add_quicklook

from si_utils.rewrite_cog import rewrite_cog


def ice_s1_product_final_editing(ice_product_input_dir, ice_product_output_dir, wekeo_geom, apply_dem_mask_file=None):
    
    product_id = os.path.basename(ice_product_input_dir)
    files_expected = set()
    for expected_sufix in ['_RLIE.tif', '_QC.tif', '_QCFLAGS.tif', '_MTD.xml']:
        file_loc = os.path.join(ice_product_input_dir, product_id + expected_sufix)
        assert os.path.exists(file_loc), 'file %s missing from product'%file_loc
        files_expected.add(product_id + expected_sufix)
        if apply_dem_mask_file is not None:
            if expected_sufix in ['_RLIE.tif', '_QC.tif', '_RS.tif']:
                apply_dem_mask(file_loc, apply_dem_mask_file)
    #remove unexpected files
    for filename in set(os.listdir(ice_product_input_dir)) - files_expected:
        os.unlink(os.path.join(ice_product_input_dir, filename))
    
    #add color tables to tif files
    add_colortable_to_si_products(ice_product_input_dir, product_tag=product_id)
    
    #transform geotiff into COG
    rewrite_cog(ice_product_input_dir, dest_path=ice_product_output_dir, verbose=1)
    
    #add quicklook
    add_quicklook(ice_product_output_dir, '_RLIE.tif')
    
    measurement_date = datetime.strptime(product_id.split('_')[1], '%Y%m%dT%H%M%S')
    
    json_dict = {
        "collection_name": "HR-S&I",
        "resto": {
            "type": "Feature",
            "geometry": {
                "wkt": wekeo_geom.simplify(0.001, preserve_topology=True).wkt
            },
            "properties": {
                "productIdentifier": None,
                "title": product_id,
                "resourceSize": 1024*int(compute_size_du(ice_product_output_dir)),
                "organisationName": "EEA",
                "startDate": measurement_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                "completionDate": measurement_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                "productType": "RLIE",
                "resolution": 20,
                "mission": 'S1',
                "processingBaseline": product_id.split('_')[-2],
                "host_base": None,
                "cloudCover": None,
                "s3_bucket": None,
                "thumbnail": None
            }}}
            
    with open('%s/dias_catalog_submit.json'%ice_product_output_dir, mode='w') as ds:
        json.dump(json_dict, ds, ensure_ascii=True, indent=4)
