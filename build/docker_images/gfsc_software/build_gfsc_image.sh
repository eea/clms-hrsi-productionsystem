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
# Docker image name
gfsc_image_name=gf


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
cd components/gf

# Use the git hash as a tag for the resulting docker gfsc image.
gfsc_image_tag=git-$(git rev-parse --short=8 HEAD)

gfsc_image="$gfsc_image_name:$gfsc_image_tag"

gfsc_image_tar_file="$gfsc_image.tar"
image_bucket_path="$remote_name:$infra_bucket/docker/images/$gfsc_image_tar_file"

log "create image $image_bucket_path..."
if [[ $(rclone lsf "$image_bucket_path") = "$gfsc_image.tar" ]] ; then
  log "image already exists in the bucket, nothing to do"
else
  log "image doesn't exist yet in the bucket"

  if docker image inspect "$gfsc_image" &> /dev/null ; then
    log "gfsc image already exist locally ($gfsc_image)"
  else
    log "gfsc image doesn't exist locally ($gfsc_image), build it"

    log "start the docker build for gfsc image..."
    docker build \
      --rm \
      -t="$gfsc_image" \
      -f="Dockerfile" \
      .
  fi

  log "extract the docker image as a tar archive and upload it to the bucket"
  docker save $gfsc_image \
    | rclone rcat "$image_bucket_path"
fi      

log "done"
