#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
try:
    from osgeo import gdal
except:
    import gdal
from si_utils.compress_geotiff import compress_geotiff_file


def unpackbits(x, num_bits):
    if np.issubdtype(x.dtype, np.floating):
        raise ValueError("numpy data type needs to be int-like")
    xshape = list(x.shape)
    x = x.reshape([-1, 1])
    mask = 2**np.arange(num_bits, dtype=x.dtype).reshape([1, num_bits])
    return (x & mask).astype(bool).astype(int).reshape(xshape + [num_bits])

def unpackbits2d(ar):
    assert np.sum(ar>255) == 0
    return unpackbits(ar, 8)
    
    
    
def packbits2d(ar):
    return np.squeeze(np.packbits(ar, axis=2, bitorder='little'), axis=2)
    
    

def bit_bandmath(output_file, raster_info_dict, source_list_per_band, no_data_values_per_band=None, compress=True, add_overviews=True, use_default_cosims_config=True):
    """Creates a copy of a monoband uint8 source model file and fills each bits independantly from different uint8 files.
    
    :param output_file: path to output TIF file
    :param raster_info_dict: dict containing some gdal.Info information on raster to create (['size'], ['geoTransform'] and ['coordinateSystem']['wkt'])
    :param source_list_per_band: list of source lists (1 per output file band)
    :param reinitialize_values: reinitialize values to 0, otherwise values from source_model_file are kept
    :param keep_no_data_from_source_file: reapply nodata values from source_model_file at the end of operations
    :return: returns nothing
    
    source list example (monoband): [{'filepath': cloud_mask_file, 'bandnumber': 1, 'bit_operations': {0: 'A[:,:,1]', 1: '1-(1-A[:,:,2])*(1-A[:,:,3])',  2: '(1-A[:,:,1])*(1-A[:,:,6])'}}]
    WARNING: bits unpacked in little endian (from smaller to greater)
    """
    
    nbands = len(source_list_per_band)
    
    #open source model file, copy it and load data
    ds_out = gdal.GetDriverByName('GTiff').Create(output_file, raster_info_dict['size'][0], raster_info_dict['size'][1], nbands, gdal.GDT_Byte)
    is_gcps = 'gcps' in raster_info_dict
    if 'gcps' in raster_info_dict:
        assert 'geoTransform' not in raster_info_dict
        ds_out.SetGCPs([gdal.GCP(el['x'], el['y'], el['z'], el['pixel'], el['line']) for el in raster_info_dict['gcps']['gcpList']], raster_info_dict['gcps']['coordinateSystem']['wkt'])
    else:
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
            local_eval_dict = {'np': np, 'logical_array_list_operation': logical_array_list_operation}
            for i_src, src in enumerate(dico['sources']):
                local_name = 'A%d'%i_src
                #get all input arrays
                assert os.path.exists(src['filepath']), 'file %s missing'%src['filepath']
                ds_loc = gdal.Open(src['filepath'])
                if ds_loc is None:
                    raise RuntimeInputFileError('gdal could not open file %s'%src['filepath'])
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
        
    if not is_gcps:
        ds_out.SetProjection(raster_info_dict['coordinateSystem']['wkt'])
    ds_out = None
    del ds_out
    if compress or add_overviews:
        txt_print = ' -> bit bandmath adding '
        if compress:
            txt_print += '+ internal compression'
        if add_overviews:
            txt_print += '+ overviews'
        print(txt_print)
        compress_geotiff_file(output_file, add_overviews=add_overviews, use_default_cosims_config=use_default_cosims_config)
        
    
    
    

