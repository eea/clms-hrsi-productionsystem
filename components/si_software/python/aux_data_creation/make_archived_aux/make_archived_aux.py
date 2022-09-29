#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *

archive_prefix = 'csi_aux'

def make_archived_aux(aux_src_dir, aux_target_dir, rclone_config_file=None, pbs_mode=False, tmp_dir=None, tile_id=None, aoi_eea39_dir=None):
    
    assert os.path.basename(aux_target_dir) == archive_prefix, 'aux_target_dir last directory must be named %s'%archive_prefix
    
    #tmp dir
    if tmp_dir is None:
        if 'TMPDIR' in os.environ:
            temp_dir_loc = os.environ['TMPDIR']
        else:
            temp_dir_loc = os.path.abspath(os.getcwd())
    else:
        temp_dir_loc = tmp_dir
    os.makedirs(temp_dir_loc, exist_ok=True)
    tmp_dir_main = os.path.abspath(tempfile.mkdtemp(prefix='tmpmaa_', dir=temp_dir_loc))
    del temp_dir_loc
    
    
    #tile processing
    if tile_id is not None:
        cwd = os.getcwd()
        os.chdir(tmp_dir_main)
        for fol in ['eu_dem', 'eu_hydro/shapefile', 'eu_hydro/raster/20m', 'hrl_qc_flags', 'tree_cover_density']:
            subprocess.check_call(['rclone', '--config=%s'%rclone_config_file, 'copy', os.path.join(aux_src_dir, fol, tile_id), os.path.join(tile_id, fol)])
        archive_file = archive_prefix + '_' + tile_id + '.tar'
        subprocess.check_call(['tar', '-cvf', archive_file, tile_id])
        subprocess.check_call(['rclone', '--config=%s'%rclone_config_file, 'sync', archive_file, aux_target_dir, '-v'])
        os.chdir(cwd)
        shutil.rmtree(tmp_dir_main)
        return
    
    
    
    #get tile list
    if aoi_eea39_dir is None:
        from si_reprocessing import install_cnes
        try:
            install_cnes.check_on_hal()
        except:
            raise Exception('aoi_eea39_dir must be given if not on CNES cluster')
        aoi_eea39_dir = os.path.join(install_cnes.reprocessing_static_data_dir, 'hidden_value', 'AOI_EEA39')
    with open(os.path.join(aoi_eea39_dir, 's2tiles_eea39', 's2tiles_eea39_gdal_info.json')) as ds:
        eea39_tile_ids = sorted(list(json.load(ds).keys()))
        
    
    #launch individual tile jobs
    if args.pbs_mode:
        
        assert tile_id is None, 'pbs_mode can only be used to launch all tiles so the tile_id argument must not be specified'
        
        from si_reprocessing import install_cnes
        install_cnes.check_on_hal()
        
        for tile_id in eea39_tile_ids:
            
            tmp_dir_loc = os.path.join(tmp_dir_main, tile_id)
            os.makedirs(tmp_dir_loc)
        
            cwd = os.getcwd()
            
            cmd = ['source %s'%install_cnes.activate_file]
            cmd_loc = [os.path.abspath(__file__), aux_src_dir, aux_target_dir, '--tile_id=%s'%tile_id]
            if rclone_config_file is not None:
                cmd_loc += ['--rclone_config_file=%s'%rclone_config_file]

            cmd += [' '.join(cmd_loc)]
            with open('%s/run.sh'%tmp_dir_loc, mode='w') as ds:
                ds.write('%s\n'%('\n'.join(cmd)))
            os.system('chmod u+x %s/run.sh'%tmp_dir_loc)
            
            txt_pbs = ['#!/bin/bash', '#PBS -N csia_%s'%tile_id, '#PBS -l select=1:ncpus=1:mem=1000mb:os=rh7', '#PBS -l walltime=1:00:00']
            txt_pbs += ['%s/run.sh'%tmp_dir_loc]
            with open('%s/run.pbs'%tmp_dir_loc, mode='w') as ds:
                ds.write('%s\n'%('\n'.join(txt_pbs)))
            os.chdir(tmp_dir_loc)
            subprocess.check_call(['qsub', 'run.pbs'])
            os.chdir(cwd)

    else:
        
        for tile_id in eea39_tile_ids:
            print(tile_id)
            tmp_dir_loc = os.path.join(tmp_dir_main, tile_id)
            os.makedirs(tmp_dir_loc)
            make_archived_aux(aux_src_dir, aux_target_dir, rclone_config_file=rclone_config_file, tmp_dir=tmp_dir_loc, tile_id=tile_id)

    

if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description="Create binary water mask rasters using S2-tiled MAJA DEMs as raster models and S2-tiled EU-Hydro shapefiles")
    parser.add_argument("aux_src_dir", type=str, help="aux source folder containing unarchived directory structure. Can be on mounted file system or on remote bucket.")
    parser.add_argument("aux_target_dir", type=str, help="aux target folder containing archived. Can be on mounted file system or on remote bucket. " + \
        "aux_target_dir last directory must be named %s"%archive_prefix)
    parser.add_argument("--rclone_config_file", type=str, help='rclone_config_file if aux_target_dir is on bucket')
    parser.add_argument("--pbs_mode", action="store_true", help='pbs_mode, submit each tile individual processing on separate jobs, makes processing a lot faster.')
    parser.add_argument("--tmp_dir", type=str, help='tmp_dir to use. If this argument is not given, $TMPDIR will be used if set. If unset, the current working directory is used.')
    parser.add_argument("--tile_id", type=str, help='tile_id, to process only this tile')
    parser.add_argument("--aoi_eea39_dir", type=str, help='aoi_eea39_dir. Must be given if --tile_id option is not used and not on CNES cluster.')
    args = parser.parse_args()
    
    make_archived_aux(args.aux_src_dir, args.aux_target_dir, rclone_config_file=args.rclone_config_file, \
        pbs_mode=args.pbs_mode, tmp_dir=args.tmp_dir, tile_id=args.tile_id, aoi_eea39_dir=args.aoi_eea39_dir)
    

