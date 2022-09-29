#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys, shutil, subprocess
import argparse

assert sys.version_info.major >= 3, 'python 3 required'

if __name__ == '__main__':
    
    file_dir = os.path.dirname(os.path.realpath(__file__))
    dockerfile = os.path.join(file_dir, 'Dockerfile_part2_arlie')
    rename_decompress = os.path.join(file_dir, 'rename_decompress.py')
    
    parser = argparse.ArgumentParser(description='This script is used to generate the docker image for the CoSIMS software. ' + \
        'It copies the Dockerfile in the docker_install_bundle and runs the docker build command.')
    parser.add_argument("docker_install_bundle", type=str, help="Path to the docker_install_bundle directory where all necessary programs installers are stored.")
    parser.add_argument("--clean", action='store_true', help="clean docker")
    parser.add_argument("--squash", action='store_true', help="squash docker")
    parser.add_argument("--force", action='store_true', help="force the build even if image name already exists")
    parser.add_argument("--final-image-name", type=str, help="docker image name for the final image", default='si_software_part2_arlie')
    parser.add_argument("--final-image-tag", type=str, help="docker image tag for the final image", default='latest')
    args = parser.parse_args()
    
    if not os.path.exists(dockerfile):
        raise Exception('No dockerfile at path %s'%dockerfile)
    if not os.path.exists(rename_decompress):
        raise Exception('No rename_decompress at path %s'%rename_decompress)
    if not os.path.exists(args.docker_install_bundle):
        raise Exception('No docker_install_bundle at path %s'%args.docker_install_bundle)
        
    #erase everything if --clean option enabled
    if args.clean:
        subprocess.check_call(['docker', 'system', 'prune', '-a'])
    
    print('Start building final image...')
    #build csi processing chain docker
    csi_image_build_path = args.docker_install_bundle
    os.chdir(csi_image_build_path)
    shutil.copy(dockerfile, 'Dockerfile')
    shutil.copy(rename_decompress, os.path.basename(rename_decompress))
    cmd = ['docker', 'build']
    if args.squash:
        cmd += ['--squash']
    cmd += ['--tag=%s:%s'%(args.final_image_name, args.final_image_tag), '.']
    subprocess.check_call(cmd)
    #delete added files
    for filename in [os.path.basename(rename_decompress), 'Dockerfile']:
        os.unlink(filename)
