#!/bin/bash

# Be sure we fail on error and output debugging information
set -e
set -o pipefail
trap 'log "error on line $LINENO"' ERR

here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


#-------------------------------------------------------------------------

# Define some utility functions

# Basic logging tool. Usage:
#   log "my message"
tool=$0
log () {
  message=$1
  # Output the log message with the tool's name and the date.
  datestring=$( date +'%Y-%m-%d %H:%M:%S' )
  echo -e "[$tool] [$datestring] $message"
}

# Utility to fail the script if one environment variable is missing.
error_on_missing_env () {
  env_var_name=$1
  if [ ! -v "$env_var_name" ]; then
    echo "$0: the required environment variable '$env_var_name' is missing"
    exit 100
  fi
}


#-------------------------------------------------------------------------

# ----- Configuration of this script
# The rclone remote name to access the infra bucket.
remote_name=csi
# The bucket where we stor archives and docker images.
infra_bucket=foo
# The name of the bundle that is needed to build the S&I docker images.
bundle_name=bundle_name
bundle_name_untar=docker_install_bundle_part2
# Docker image names
part2_image_name=si_software_part2


log "start..."

# ----- Check the rclone config
if rclone listremotes | grep -q "^$remote_name:$" ; then
  log "found the rclone remote storage which name is $remote_name "
else
  log "Error: can't find the rclone remote storage which name is $remote_name"
  log "please check this remote is properly configured in the rclone config file."
  # Show the default config file for rclone
  rclone config file
  exit 1
fi

if rclone lsd $remote_name:$infra_bucket &> /dev/null ; then
  log "bucket $infra_bucket found on remote $remote_name"
else
  log "Error: can't find bucket $infra_bucket on remote $remote_name"
  exit 1
fi


# ----- Start the script
# Go to the base directory of the project
cd "$here/../../.."
csi_root_dir=$(pwd)
# Go to the directory where to build docker images
cd components/si_software/docker

# Use the git hash as a tag for the resulting docker final image.
part2_image_tag=git-$(git rev-parse --short=8 HEAD)

part2_image="$part2_image_name:$part2_image_tag"

part2_image_tar_file="$part2_image.tar"
image_bucket_path="$remote_name:$infra_bucket/docker/images/$part2_image_tar_file"

log "create image $image_bucket_path..."
if [[ $(rclone lsf "$image_bucket_path") = "$part2_image.tar" ]] ; then
  log "image already exists in the bucket, nothing to do"
else
  log "image doesn't exist yet in the bucket"

  if docker image inspect "$part2_image" &> /dev/null ; then
    log "part2 image already exist locally ($part2_image)"
  else
    log "part2 image doesn't exist locally ($part2_image), build it"

    if [[ -d $bundle_name ]]; then
      log "S&I software dependencies bundle exists"
    else
      log "S&I software dependencies bundle is missing, download it..."
      # Get the bundle from the bucket and untar it in the local directory
      rclone \
        cat \
        "$remote_name:$infra_bucket/si_software_bundles/$bundle_name.tar" \
        | tar xf -
    fi

    log "start the docker build for final image..."
    python3 ./make_docker_file_part2.py \
      --csi_root_dir="$csi_root_dir" \
      --squash \
      --force \
      --final-image-name="$part2_image_name" \
      --final-image-tag="$part2_image_tag" \
      $(pwd)/$bundle_name_untar
  fi

  log "extract the docker image as a tar archive and upload it to the bucket"
  docker save $part2_image \
    | rclone rcat "$image_bucket_path"
fi      

log "done"
