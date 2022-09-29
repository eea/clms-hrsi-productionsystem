#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys, shutil, subprocess
import argparse

assert sys.version_info.major >= 3, 'python 3 required'

file_dir = os.path.dirname(os.path.realpath(__file__))
dockerfile_protoimage = os.path.join(file_dir, 'Dockerfile_protoimage_otb_snap')
dockerfile = os.path.join(file_dir, 'Dockerfile')
rename_decompress = os.path.join(file_dir, 'rename_decompress.py')
default_csi_root_dir = os.path.dirname(os.path.dirname(os.path.dirname(file_dir)))
default_target_dir = os.path.abspath(os.path.join(os.getcwd(), 'si_software_bundle_cnes'))
base_image_tag = 'si_software_base'
final_image_tag = 'si_software'


def make_cnes_bundle(target_dir, csi_root_dir, docker_install_bundle):
    
    os.makedirs(target_dir)
    for el in ['Dockerfile']
    shutil.copy(el, os.path.join(target_dir, el))
    
    
    
    

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='This script is used to generate a docker bundle for si_software part 1 for CNES')
    parser.add_argument("--target_dir", type=str, help="default target directory for CNES", default=default_target_dir)
    parser.add_argument("--csi_root_dir", type=str, help="Path to the CoSIMS root GIT directory. Default is %s"%default_csi_root_dir, default=default_csi_root_dir)
    parser.add_argument("--docker_install_bundle", type=str, help="Path to the docker_install_bundle directory where all necessary programs installers are stored.")
    args = parser.parse_args()
    
    if not os.path.exists(dockerfile_protoimage):
        raise Exception('No dockerfile for OTB/SNAP protoimage at path %s'%dockerfile_protoimage)
    if not os.path.exists(dockerfile):
        raise Exception('No dockerfile at path %s'%dockerfile)
    if not os.path.exists(rename_decompress):
        raise Exception('No rename_decompress at path %s'%rename_decompress)
    if not os.path.exists(args.docker_install_bundle):
        raise Exception('No docker_install_bundle at path %s'%args.docker_install_bundle)
        
    #erase everything if --clean option enabled
    if args.clean:
        subprocess.check_call(['docker', 'system', 'prune', '-a'])
    
    if not args.only_final_image:
        print('Start building base image...')
        #build protoimage => OTB is very long to install so we pre-install it in a separate image to avoid reinstalling it when using the --squash option
        protoimage_already_exists = False
        docker_image_list_lines = [line for line in subprocess.check_output(['docker', 'image', 'list']).decode('utf-8').split('\n')]
        if len(docker_image_list_lines) > 1:
            docker_image_names = set([el.split()[0] for el in docker_image_list_lines[1:] if len(el) > 0])
            if args.base_image_name in docker_image_names:
                protoimage_already_exists = True
        if protoimage_already_exists and not args.force:
            print('Found an existing OTB/SNAP protoimage, skipping protoimage build')
        else:
            protoimage_build_path = os.path.join(args.docker_install_bundle, 'protoimage_otb_snap')
            os.chdir(protoimage_build_path)
            shutil.copy(dockerfile_protoimage, 'Dockerfile')
            shutil.copy(rename_decompress, os.path.basename(rename_decompress))
            cmd = ['docker', 'build']
            cmd += ['--squash']
            cmd += ['--tag=%s:%s'%(args.base_image_name, args.base_image_tag), '.']
            subprocess.check_call(cmd)
            #delete added files
            for filename in [os.path.basename(rename_decompress), 'Dockerfile']:
                os.unlink(filename)
    
    if not args.only_base_image:
        print('Start building final image...')
        #build csi processing chain docker
        csi_image_build_path = os.path.join(args.docker_install_bundle, 'csi_processing_chain_softwares')
        os.chdir(csi_image_build_path)
        shutil.copy(dockerfile, 'Dockerfile')
        shutil.copy(rename_decompress, os.path.basename(rename_decompress))
        if os.path.isdir(args.csi_root_dir):
            csi_software_copied_filename = 'csi_si_software.tar.gz'
            os.chdir(os.path.dirname(args.csi_root_dir))
            subprocess.check_call(['tar', '-zcvf', os.path.join(csi_image_build_path, csi_software_copied_filename), 
                'cosims/components/common/python', 
                'cosims/components/si_software/python'])
            os.chdir(csi_image_build_path)
        else:
            sufix = '.'.join(os.path.basename(args.csi_root_dir).split('.')[1:])
            csi_software_copied_filename = 'csi_si_software.' + sufix
            shutil.copy(args.csi_root_dir, csi_software_copied_filename)
        cmd = ['docker', 'build']
        if args.squash:
            cmd += ['--squash']
        cmd += ['--tag=%s:%s'%(args.final_image_name, args.final_image_tag), '.']
        cmd += ['--build-arg', f'BASE_IMAGE_TAG={args.base_image_tag}']
        subprocess.check_call(cmd)
        #delete added files
        for filename in [os.path.basename(rename_decompress), csi_software_copied_filename, 'Dockerfile']:
            os.unlink(filename)
