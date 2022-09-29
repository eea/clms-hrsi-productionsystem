#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, shutil
import tempfile
import numpy as np
try:
    from osgeo import gdal
except:
    import gdal

def compress_geotiff_file(src_file, dest_file=None, tiled=True, compress='deflate', zlevel=4, predictor=1, add_overviews=False, use_default_cosims_config=False):
    
    if dest_file is None:
        dest_file = src_file
        
    if add_overviews:
        # build external overviews base on src image
        tmp_img = gdal.Open(src_file, 0) # 0 = read-only (external overview), 1 = read-write.
        gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
        if use_default_cosims_config:
            gdal.SetConfigOption('GDAL_TIFF_OVR_BLOCKSIZE', '256')
        tmp_img.BuildOverviews("NEAREST", [2,4,8,16,32])
        del tmp_img
        
    compression_options = []
    if tiled:
        compression_options.append('tiled=yes')
        if use_default_cosims_config:
            compression_options.append('blockxsize=256')
            compression_options.append('blockysize=256')
    compression_options.append('compress=%s'%compress)
    if compress == 'deflate':
        compression_options.append('zlevel=%d'%zlevel)
    if compress in ['deflate', 'lzw', 'lzma']:
        compression_options.append('predictor=%d'%predictor)
    # any src_file overviews will be internal in dest_file
    compression_options.append('COPY_SRC_OVERVIEWS=YES')
    if os.path.abspath(src_file) == os.path.abspath(dest_file):
        prefix, sufix = '.'.join(dest_file.split('.')[0:-1]), dest_file.split('.')[-1]
        temp_name = prefix + '_temp.' + sufix
        gdal.Translate(temp_name, src_file, creationOptions=compression_options)
        shutil.move(temp_name, dest_file)
    else:
        gdal.Translate(dest_file, src_file, creationOptions=compression_options)

    if os.path.exists(src_file + '.ovr'):
        # remove temporary external overview
        os.unlink(src_file + '.ovr')



def unpackbits2d(ar):
    return np.unpackbits(np.expand_dims(ar, axis=2), axis=2, bitorder='little')
    
    
    
def packbits2d(ar):
    return np.squeeze(np.packbits(ar, axis=2, bitorder='little'), axis=2)
    
    

def bit_bandmath(output_file, raster_info_dict, source_list_per_band, no_data_values_per_band=None, compress=True, add_overviews=True, use_default_cosims_config=True):
    """Creates a copy of a monoband uint8 source model file and fills each bits independantly from different uint8 files.
    
    :param output_file: path to output TIF file
    :param raster_info_dict: dict containing some gdal.Info information on raster to create (['size'], ['geoTransform'] and ['coordinateSystem']['wkt']) or sample file
    :param source_list_per_band: list of source lists (1 per output file band)
    :param reinitialize_values: reinitialize values to 0, otherwise values from source_model_file are kept
    :param keep_no_data_from_source_file: reapply nodata values from source_model_file at the end of operations
    :return: returns nothing
    
    source list example (monoband): [{'filepath': cloud_mask_file, 'bandnumber': 1, 'bit_operations': {0: 'A[:,:,1]', 1: '1-(1-A[:,:,2])*(1-A[:,:,3])',  2: '(1-A[:,:,1])*(1-A[:,:,6])'}}]
    WARNING: bits unpacked in little endian (from smaller to greater)
    """
    
    if not isinstance(raster_info_dict, dict):
        assert os.path.exists(raster_info_dict)
        raster_info_dict = gdal.Info(raster_info_dict, format='json')
    
    nbands = len(source_list_per_band)
    
    #open source model file, copy it and load data
    ds_out = gdal.GetDriverByName('GTiff').Create(output_file, raster_info_dict['size'][0], raster_info_dict['size'][1], nbands, gdal.GDT_Byte)
    ds_out.SetGeoTransform(tuple(raster_info_dict['geoTransform']))
    for band_no in range(nbands):
        outband = ds_out.GetRasterBand(band_no+1)
        outband.DeleteNoDataValue()
        if no_data_values_per_band is not None:
            if no_data_values_per_band[band_no] is not None:
                outband.SetNoDataValue(float(no_data_values_per_band[band_no]))
        output_array = outband.ReadAsArray()
        output_array[:,:] = 0

        output_bits_unpacked = False
        for dico in source_list_per_band[band_no]:
            local_eval_dict = {'np': np}
            for i_src, src in enumerate(dico['sources']):
                local_name = 'A%d'%i_src
                #get all input arrays
                assert os.path.exists(src['filepath']), 'file %s missing'%src['filepath']
                ds_loc = gdal.Open(src['filepath'])
                if ds_loc is None:
                    raise Exception('gdal could not open file %s'%src['filepath'])
                local_eval_dict[local_name] = ds_loc.GetRasterBand(src['bandnumber']).ReadAsArray()
                if src['unpack_bits']:
                    local_eval_dict[local_name] = unpackbits2d(local_eval_dict[local_name])
                ds_loc = None            
            
            #fill output_array
            is_bit_operation_on_output_array = isinstance(dico['operation'], dict)
            if is_bit_operation_on_output_array:
                if not output_bits_unpacked:
                    output_array = unpackbits2d(output_array)
                    output_bits_unpacked = True
                local_eval_dict['B'] = output_array
                for id_bit, operation in dico['operation'].items():
                    output_array[:,:,id_bit] = eval(operation, {}, local_eval_dict)
            else:
                if output_bits_unpacked:
                    output_array = packbits2d(output_array)
                    output_bits_unpacked = False
                local_eval_dict['B'] = output_array
                output_array = eval(dico['operation'], {}, local_eval_dict)

        if output_bits_unpacked:
            output_array = packbits2d(output_array)

        #write array
        outband.WriteArray(output_array)
        outband.FlushCache()
        
    ds_out.SetProjection(raster_info_dict['coordinateSystem']['wkt'])
    ds_out = None
    del ds_out
    if compress or add_overviews:
        compress_geotiff_file(output_file, add_overviews=add_overviews, use_default_cosims_config=use_default_cosims_config)
        

# ~ def edit_lis_fsc_qc_layers(lis_input_folder, l2a_path, water_mask_path, tcd_path, output_folder=None):
    
    # ~ maja_product_tag = os.path.basename(l2a_path)
    # ~ lis_product_tag = os.path.basename(lis_input_folder)
    # ~ if output_folder is None:
        # ~ output_folder = lis_input_folder
    # ~ os.makedirs(output_folder, exist_ok=True)
    # ~ fsc_toc_file = check_path(os.path.join(lis_input_folder, '%s_FSCTOC.tif'%lis_product_tag))
    # ~ fsc_og_file = check_path(os.path.join(lis_input_folder, '%s_FSCOG.tif'%lis_product_tag))
    # ~ maja_cloud_mask_file = check_path(os.path.join(l2a_path, 'MASKS', '%s_CLM_R2.tif'%maja_product_tag))
    # ~ geophysical_mask_file = check_path(os.path.join(l2a_path, 'MASKS', '%s_MG2_R2.tif'%maja_product_tag))
    # ~ raster_gdal_info = gdal.Info(fsc_toc_file, format='json')
    
    # ~ #cloud : there used to be a specific cloud processing but instead we are just copying the MAJA cloud mask
    # ~ output_cloud_file = '%s/%s_CLD.tif'%(output_folder, lis_product_tag)
    # ~ copy_original(maja_cloud_mask_file, output_cloud_file)
    # ~ compress_geotiff_file(output_cloud_file, add_overviews=True, use_default_cosims_config=True)
    
    # ~ #expert flags
    # ~ source_list = []
    # ~ #bit 0: MAJA sun too low for an accurate slope correction
    # ~ #bit 1: MAJA sun tangent
    # ~ source_list += [{'sources': [{'filepath': geophysical_mask_file, 'bandnumber': 1, 'unpack_bits': True}], \
        # ~ 'operation': {0: 'A0[:,:,6]', 1: 'A0[:,:,7]'}}]
    # ~ #bit 2: water mask
    # ~ source_list += [{'sources': [{'filepath': water_mask_path, 'bandnumber': 1, 'unpack_bits': False}], \
        # ~ 'operation': {2: 'A0==1'}}]
    # ~ #bit 3: tree cover density > 90%
    # ~ source_list += [{'sources': [{'filepath': tcd_path, 'bandnumber': 1, 'unpack_bits': False}], \
        # ~ 'operation': {3: 'np.logical_and(A0<101,A0>90)'}}]
    # ~ #bit 4: snow detected under thin clouds
    # ~ source_list += [{'sources': [{'filepath': maja_cloud_mask_file, 'bandnumber': 1, 'unpack_bits': False}, {'filepath': fsc_toc_file, 'bandnumber': 1, 'unpack_bits': False}], \
        # ~ 'operation': {4: '(A0>0)*(A1>0)*(A1<101)'}}]
    # ~ #bit 5: tree cover density undefined or unavailable
    # ~ source_list += [{'sources': [{'filepath': tcd_path, 'bandnumber': 1, 'unpack_bits': False}], \
        # ~ 'operation': {5: 'A0>100'}}]
    # ~ output_expert_file = '%s/%s_QCFLAGS.tif'%(output_folder, lis_product_tag)
    # ~ bit_bandmath(output_expert_file, raster_gdal_info, [source_list], compress=True, use_default_cosims_config=True)
    
def apply_dem_mask(input_raster, dem_file):
    
    ds = gdal.Open(dem_file)
    dem_data = np.ma.masked_invalid(ds.GetRasterBand(1).ReadAsArray())
    no_data_value = ds.GetRasterBand(1).GetNoDataValue()
    dem_data.mask[dem_data==no_data_value] = True
    ds = None
    del ds
    
    ds = gdal.Open(input_raster, 1)
    band = ds.GetRasterBand(1)
    no_data_value = band.GetNoDataValue()
    input_data = band.ReadAsArray()
    input_data[dem_data.mask] = no_data_value
    band.WriteArray(input_data)
    ds = None
    del ds
    
    compress_geotiff_file(input_raster)
    
    
def change_product_version(product_path, version_tag):
    #check old version tag format
    old_version_tag = os.path.basename(product_path).split('_')[-2]
    assert len(old_version_tag) == 4
    assert old_version_tag[0] == 'V'
    assert set(old_version_tag[1:]).issubset(set(['%d'%ii for ii in range(10)]))
    #check new version tag format
    assert len(version_tag) == 4
    assert version_tag[0] == 'V'
    assert set(version_tag[1:]).issubset(set(['%d'%ii for ii in range(10)]))
    
    #copy file to new paths
    new_product_path = os.path.join(os.path.dirname(product_path), os.path.basename(product_path).replace('_%s_'%old_version_tag, '_%s_'%version_tag))
    assert new_product_path != product_path
    os.makedirs(new_product_path)
    for el in os.listdir(product_path):
        shutil.copy(os.path.join(product_path, el), os.path.join(new_product_path, el.replace('_%s_'%old_version_tag, '_%s_'%version_tag)))
    
    #change _MTD.xml file
    mtdxml_file = os.path.join(new_product_path, os.path.basename(new_product_path) + '_MTD.xml')
    assert os.path.exists(mtdxml_file)
    with open(mtdxml_file) as ds:
        txt = ds.read()
    txt_out = txt.replace('_%s_'%old_version_tag, '_%s_'%version_tag)
    with open(mtdxml_file, mode='w') as ds:
        ds.write(txt_out)
    
    #change json file (if it exists)
    json_file = os.path.join(new_product_path, 'dias_catalog_submit.json')
    if os.path.exists(json_file):
        with open(json_file) as ds:
            txt = ds.read()
        txt_out = txt.replace('_%s_'%old_version_tag, '_%s_'%version_tag)
        with open(json_file, mode='w') as ds:
            ds.write(txt_out)
    
    return new_product_path
    
    
        
def correct_fsc_qc_layers(input_path, dem_dir_path, tcd_path, output_path=None, temp_dir=None):
    
    dem_file_path = os.path.join(dem_dir_path, os.path.basename(dem_dir_path) + '.DBL.DIR', 'dem_20m.tif')
    lis_product_tag = os.path.basename(input_path)
    fsc_toc_file = os.path.join(input_path, '%s_FSCTOC.tif'%lis_product_tag)
    fsc_og_file = os.path.join(input_path, '%s_FSCOG.tif'%lis_product_tag)
    qc_expert_file = os.path.join(input_path, '%s_QCFLAGS.tif'%lis_product_tag)
    assert os.path.exists(fsc_toc_file), 'file %s not found'%fsc_toc_file
    assert os.path.exists(fsc_og_file), 'file %s not found'%fsc_og_file
    assert os.path.exists(qc_expert_file), 'file %s not found'%qc_expert_file
    
    raster_gdal_info = gdal.Info(fsc_toc_file, format='json')
    
    if temp_dir is None:
        if 'TMPDIR' in os.environ:
            temp_dir = os.environ['TMPDIR']
        else:
            temp_dir = '.'
    os.makedirs(temp_dir, exist_ok=True)
    temp_dir_session = tempfile.mkdtemp(dir=temp_dir, prefix='corrfsc_')
    
    if output_path is not None:
        if os.path.exists(output_path):
            assert len(os.listdir(output_path)) == 0, 'output dir %s must be empty'%output_path
    
    try:
        
        #apply DEM mask on FSC TOC file
        output_fsc_toc_file = os.path.join(temp_dir_session, '%s_FSCTOC.tif'%lis_product_tag)
        shutil.copy(fsc_toc_file, output_fsc_toc_file)
        apply_dem_mask(output_fsc_toc_file, dem_file_path)
        
        #apply DEM mask on FSC OG file
        output_fsc_og_file = os.path.join(temp_dir_session, '%s_FSCOG.tif'%lis_product_tag)
        shutil.copy(fsc_og_file, output_fsc_og_file)
        apply_dem_mask(output_fsc_og_file, dem_file_path)
    
        #QC layer top of canopy
        #[0: highest quality, 1: lower quality, 2: decreasing quality, 3: lowest quality, 205: cloud mask, 255: no data]
        source_list = []
        source_list += [{'sources': [{'filepath': qc_expert_file, 'bandnumber': 1, 'unpack_bits': True}], \
            'operation': 'np.minimum(B*0+3,(4-np.maximum(B*0, (100.-30.*A0[:,:,1]-50.*A0[:,:,0]-25.*A0[:,:,4]-25.*A0[:,:,2])/25.))).astype(np.uint8)'}]
        #values 205 and 255 from FSCTOC snow product
        source_list += [{'sources': [{'filepath': output_fsc_toc_file, 'bandnumber': 1, 'unpack_bits': False}], \
            'operation': 'B*(A0!=205)*(A0!=255) + 205*(A0==205) + 255*(A0==255)'}]
        output_qc_toc_file = os.path.join(temp_dir_session, '%s_QCTOC.tif'%lis_product_tag)
        bit_bandmath(output_qc_toc_file, raster_gdal_info, [source_list], no_data_values_per_band=[np.uint8(255)], compress=True, use_default_cosims_config=True)
    
        #QC layer on ground
        #[0: highest quality, 1: lower quality, 2: decreasing quality, 3: lowest quality, 205: cloud mask, 255: no data]
        source_list = []
        source_list += [{'sources': [{'filepath': qc_expert_file, 'bandnumber': 1, 'unpack_bits': True}, {'filepath': tcd_path, 'bandnumber': 1, 'unpack_bits': False}], \
            'operation': 'np.minimum(B*0+3,(4-np.maximum(B*0, (100.-30.*A0[:,:,1]-50.*A0[:,:,0]-25.*A0[:,:,4]-25.*A0[:,:,2]-80.*A1)/25.))).astype(np.uint8)'}]
        #values 205 and 255 from FSCOG snow product
        source_list += [{'sources': [{'filepath': output_fsc_og_file, 'bandnumber': 1, 'unpack_bits': False}], \
            'operation': 'B*(A0!=205)*(A0!=255) + 205*(A0==205) + 255*(A0==255)'}]
        output_qc_og_file = os.path.join(temp_dir_session, '%s_QCOG.tif'%lis_product_tag)
        bit_bandmath(output_qc_og_file, raster_gdal_info, [source_list], no_data_values_per_band=[np.uint8(255)], compress=True, use_default_cosims_config=True)
    
    
        if output_path is None:
            #inplace
            shutil.move(output_fsc_toc_file, os.path.join(input_path, '%s_FSCTOC.tif'%lis_product_tag))
            shutil.move(output_fsc_og_file, os.path.join(input_path, '%s_FSCOG.tif'%lis_product_tag))
            shutil.move(output_qc_toc_file, os.path.join(input_path, '%s_QCTOC.tif'%lis_product_tag))
            shutil.move(output_qc_og_file, os.path.join(input_path, '%s_QCOG.tif'%lis_product_tag))
        else:
            lis_output_product_tag = os.path.basename(output_path)
            assert '_'.join(lis_product_tag.split('_')[0:4] + lis_product_tag.split('_')[5:]) == \
                '_'.join(lis_output_product_tag.split('_')[0:4] + lis_output_product_tag.split('_')[5:]), 'only version information may be changed in product name'
            os.makedirs(output_path, exist_ok=True)
            for filename in os.listdir(input_path):
                if any([el not in filename for el in ['_FSCTOC.tif', '_FSCOG.tif', '_QCTOC.tif', '_QCOG.tif']]):
                    shutil.copy(os.path.join(input_path, filename), os.path.join(output_path, filename.replace(lis_product_tag, lis_output_product_tag)))
            shutil.move(output_fsc_toc_file, os.path.join(output_path, lis_output_product_tag + '_FSCTOC.tif'))
            shutil.move(output_fsc_og_file, os.path.join(output_path, lis_output_product_tag + '_FSCOG.tif'))
            shutil.move(output_qc_toc_file, os.path.join(output_path, lis_output_product_tag + '_QCTOC.tif'))
            shutil.move(output_qc_og_file, os.path.join(output_path, lis_output_product_tag + '_QCOG.tif'))
    except:
        if output_path is not None:
            if os.path.exists(output_path):
                shutil.rmtree(output_path)
        raise
            
    finally:
        
        shutil.rmtree(temp_dir_session)
    
    
    
    
    
########################################
if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='This script is used to correct the QC layers of a FSC product.')
    parser.add_argument("--input", type=str, required=True, help='input FSC product')
    parser.add_argument("--dem_dir_path", type=str, required=True, help='DEM dir path')
    parser.add_argument("--tcd_path", type=str, required=True, help='TCD path')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--output", type=str, help='output FSC product. If this parameter is not filled, --inplace option must be used')
    group.add_argument("--inplace", action='store_true', help='correct QC layers in place')
    parser.add_argument("--temp_dir", type=str, help='temp directory')
    args = parser.parse_args()
    
    correct_fsc_qc_layers(args.input, args.dem_dir_path, args.tcd_path, output_path=args.output, temp_dir=args.temp_dir)

