#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys, shutil
try:
    from osgeo import gdal
except:
    import gdal
import tempfile



def compress_geotiff_file(src_file, dest_file=None, tiled=True, compress='deflate', zlevel=4, predictor=1, add_overviews=False, use_default_cosims_config=False):
    
    if dest_file is None:
        dest_file = src_file
        
    if add_overviews:
        # build external overviews base on src image
        tmp_img = gdal.Open(src_file, 0) # 0 = read-only (external overview), 1 = read-write.
        gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
        if use_default_cosims_config:
            gdal.SetConfigOption('GDAL_TIFF_OVR_BLOCKSIZE', '1024')
        tmp_img.BuildOverviews("NEAREST", [2,4,8,16,32])
        del tmp_img
        
    compression_options = []
    if tiled:
        compression_options.append('tiled=yes')
        if use_default_cosims_config:
            compression_options.append('blockxsize=1024')
            compression_options.append('blockysize=1024')
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
    

def replicate_directory_with_compressed_tiffs(input_fol, output_fol=None, add_prefix=None, tiled=True, compress='deflate', zlevel=4, predictor=1, \
    add_overviews=False, use_default_cosims_config=False, verbose=0):
    
    in_place_mode = output_fol is None
    
    #if input is a file, only convert this single file
    if os.path.isfile(input_fol):
        if in_place_mode:
            output_fol = input_fol
        compress_geotiff_file(input_fol, dest_file=output_fol, tiled=tiled, compress=compress, zlevel=zlevel, predictor=predictor, \
            add_overviews=add_overviews, use_default_cosims_config=use_default_cosims_config)
        return
        
    #parse directory and convert all geotiff files
    if add_prefix is None:
        add_prefix = ''
    if in_place_mode:
        output_fol = tempfile.mkdtemp(prefix=os.path.basename(input_fol) + '_temp_', dir=os.path.dirname(input_fol))
    if os.path.exists(output_fol):
        shutil.rmtree(output_fol)
    os.makedirs(output_fol)
    for root_src, dirs, files in os.walk(input_fol, topdown=True):
        root_target = root_src.replace(input_fol, output_fol)
        for name in files:
            src_file = os.path.join(root_src, name)
            target_file = os.path.join(root_target, add_prefix + name)
            if os.path.exists(target_file):
                continue
            if verbose > 0:
                print('%s -> %s'%(src_file, target_file))
            if src_file.lower().split('.')[-1] in ['tiff', 'tif']:
                compress_geotiff_file(src_file, dest_file=target_file, tiled=tiled, compress=compress, zlevel=zlevel, predictor=predictor, \
                    add_overviews=add_overviews, use_default_cosims_config=use_default_cosims_config)
            else:
                shutil.copy(src_file, target_file)
        for name in dirs:
            target_dir = os.path.join(root_target, name)
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
    if in_place_mode:
        shutil.rmtree(input_fol)
        shutil.move(output_fol, input_fol)
        
        
def add_line_breaks(string_in, paragraph_max_len=50):
    if paragraph_max_len < 5:
        raise InnerArgError('paragraph_max_len must be >= 5')
    if string_in.count('\n') > 0:
        return '\n'.join([add_line_breaks(line, paragraph_max_len=paragraph_max_len) for line in string_in.split('\n')])
    words = string_in.split(' ')
    istr = 0
    iword = 0
    current_line = None
    lines = []
    while(True):
        if iword >= len(words):
            if current_line is not None:
                lines.append(current_line)
            break
        if current_line is None:
            #first word of new line
            current_line = words[iword]
            iword += 1
            continue
        if len(current_line)+1+len(words[iword]) <= paragraph_max_len:
            #add word to new line since it does not exceed paragraph length
            current_line += ' ' + words[iword]
            iword += 1
            continue
        #exceeds paragraph length, new line must be created
        lines.append(current_line)
        current_line = None
    return '\n'.join(lines)
        
    
    
        
if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description="Compress all TIFF files within a folder", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("input", type=str, help="path to input directory or file")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--output", type=str, help="path to output directory or file")
    group.add_argument("--inplace", action='store_true', default=False, help=add_line_breaks('In place compression in input_folder.This is not recommended since it does not provide ' + \
        'a means to save disk space during processing : it creates all new data in a temporary output_folder and then deletes the input_folder and moves the temporary ' + \
        'output_folder in place of the input_folder. It is therefore only a convenience option. If this option is used and output_folder specified, an error is raised.'))
    parser.add_argument("--tiled", type=str, choices=['yes', 'no'], help='yes/no, Using this option will produce a tiled output', default='yes')
    parser.add_argument("--compress", type=str, help='Compression type : default is deflate', default='deflate')
    parser.add_argument("--zlevel", type=int, help='Compression level [1,9] if using the deflate compression, default=4', default=4)
    parser.add_argument("--predictor", type=int, help='Prediction type : 1 (no predictor), 2 is horizontal differencing and 3 is floating point prediction. " + \
        "Only works with deflate, lzw or zstd compressions, default=1 (no prediction)', default=1)
    parser.add_argument("--add_overviews", action='store_true', help='add overviews to geotiff')
    parser.add_argument("--use_default_cosims_config", action='store_true', help='use_default_cosims_config')
    args = parser.parse_args()
    
    args.tiled = args.tiled == 'yes'
    
    if (args.output is None) and (not args.inplace):
        raise MainArgError('Output is not specified. To transform inplace, use the --inplace option.')
    replicate_directory_with_compressed_tiffs(args.input, output_fol=args.output, tiled=args.tiled, compress=args.compress, zlevel=args.zlevel, \
        predictor=args.predictor, add_overviews=args.add_overviews, use_default_cosims_config=args.use_default_cosims_config)
    
    
