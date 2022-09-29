#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from si_common.common_functions import *
from si_report.generate_monthly_report import load_aoi_info
from si_report.report_plots import tile_map




def plot_tile_priority_list(output_figure, eea39_dir, tile_list=None, tile_list_file=None):
    
    tile_info = load_aoi_info(eea39_dir)
    if tile_list is None:
        assert tile_list_file is not None
        with open(tile_list_file) as ds:
            tile_list = [el.replace('\n','') for el in ds.readlines() if len(el) > 1]
    else:
        assert tile_list is not None
    values_dict = {el: 0 for el in tile_info['eea39_tile_list']}
    for el in tile_list:
        values_dict[el] = 1
    
    tile_map(output_figure, values_dict, tile_info, vmin=0, vmax=1, title='Priority tiles', xlabel=None, alpha_polygons=0.5, cmap_type='jet', compress_image=True, show_simulated_text=False)


if __name__ == '__main__':
            
    import argparse
    parser = argparse.ArgumentParser(description="plot priority tiles", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--tile_list_file", type=str, required=True, help="tile_list_file")
    parser.add_argument("--eea39_dir", type=str, required=True, help="eea39_dir")
    parser.add_argument("--output_figure", type=str, required=True, help="output_figure")
    args = parser.parse_args()

    plot_tile_priority_list(args.output_figure, args.eea39_dir, tile_list_file=args.tile_list_file)
    
    
    
