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
bundle_name=docker_install_bundle_12
# Docker name for the base image
base_image_name=si_software_base


log "start"

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


# Use the git hash as a tag for the resulting docker base image.
base_image_tag=git-$(git rev-parse --short=8 HEAD)

base_image="$base_image_name:$base_image_tag"

base_image_tar_file="$base_image.tar"
base_image_bucket_path="$remote_name:$infra_bucket/docker/images/$base_image_tar_file"

if docker image inspect "$base_image" &> /dev/null ; then
  log "base image already exist locally ($base_image)"
else
  log "base image doesn't exist locally ($base_image)"
  echo $base_image_bucket_path
  if [[ $(rclone lsf "$base_image_bucket_path") = "$base_image_tar_file" ]] ; then
    log "image already exists in the bucket, download it and load it into docker"
    rclone cat "$base_image_bucket_path" \
      | docker load
  else
    log "image doesn't exist yet in the bucket, build it"

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

    log "start the docker build for base image..."
    python3 ./make_docker_file.py \
      --csi_root_dir="$csi_root_dir" \
      --squash \
      --only-base-image \
      --force \
      --base-image-name="$base_image_name" \
      --base-image-tag="$base_image_tag" \
      $(pwd)/$bundle_name

    log "extract the docker image as a tar archive and upload it to the bucket"
    docker save $base_image \
      | rclone rcat "$base_image_bucket_path"
  fi
fi

if [[ $(rclone lsf "$base_image_bucket_path") = "$base_image_tar_file" ]] ; then
  log "image already exists in the bucket, no need to upload it"
else
  log "image doesn't exist in the bucket"
  log "extract the local docker image as a tar archive and upload it to the bucket"
  docker save $base_image \
    | rclone rcat "$base_image_bucket_path"
fi

log "done"
