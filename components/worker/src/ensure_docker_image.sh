#!/bin/bash

internal_error_status_code=1
external_error_status_code=2

# Be sure we fail on error and output debugging information
set -e
set -o pipefail
trap 'log "error on line $LINENO" ; exit $internal_error_status_code' ERR

here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


#-------------------------------------------------------------------------

# Define some utility functions

# Basic logging tool. Usage:
#   log "my message"
tool=$(basename $0)
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
    log "the required environment variable '$env_var_name' is missing"
    exit $internal_error_status_code
  fi
}


#-------------------------------------------------------------------------

if [ "$#" -ne 1 ]; then
  log "wrong number of arguments"
  log "usage: $0 docker_image"
  exit $internal_error_status_code
fi

docker_image=$1

# Everything is ok, proceed.

#-------------------------------------------------------------------------

# ----- Configuration of this script
# The rclone remote name to access the infra bucket.
remote_name=csi
# The bucket where we store archives and docker images.
infra_bucket=foo

log "ensure that docker image \"$docker_image\" is available locally..."

image_tar_file="$docker_image.tar"
image_bucket_path="$remote_name:$infra_bucket/docker/images/$image_tar_file"

# Check if the docker image is available locally. If not try to get it either by
# directly pulling it or load it from a tar file in the infrastructure bucket of
# the project.
if docker image inspect "$docker_image" &> /dev/null ; then
  log "local docker image $docker_image is available"
else
  log "no available local docker image named $docker_image"
  log "try to pull docker image..."
  if docker pull "$docker_image" &> /dev/null ; then
    log "docker image succefully pulled"
  else
    log "can't pull docker image (pull error code: $?), try to download it from the infra bucket"

    # TODO We comment the following because sometimes it fails to detect csi
    # even though it is properly set in rclone.conf. This raises to much errors,
    # especially during workers boot up, so we prefer to skip it. Either find
    # and correct what is going on, or the eventual refactor in Python of this
    # docker image download will make this problem obsolete.
    #
    # log "first check the rclone config..."
    # if rclone listremotes | grep -q "^$remote_name:$" ; then
    #   log "found the rclone remote storage which name is $remote_name "
    # else
    #   log "Error: can't find the rclone remote storage which name is $remote_name"
    #   log "please check this remote is properly configured in the rclone config file:"
    #   # Show the default config file for rclone
    #   config_file=$(rclone config file | tail -1)
    #   log "  configuration file name is $config_file"
    #   # In principle I thougt it could be considered as an internal error, but
    #   # successive run of this script, won't always raise the same error and at
    #   # some time the configuration is OK. I can't explain why.
    #   #
    #   # But, this is way we mark this as external error so that the caller of
    #   # this script can know it can try to call it again.
    #   exit $external_error_status_code
    # fi

    if rclone lsd "$remote_name:$infra_bucket" &> /dev/null ; then
      log "bucket $infra_bucket found on remote $remote_name"
    else
      log "Error: can't find bucket $infra_bucket on remote $remote_name"
      exit $external_error_status_code
    fi

    log "rclone config is OK"

    log "check if image is available in the bucket with path $image_bucket_path"
    if [[ $(rclone lsf "$image_bucket_path") = "$docker_image.tar" ]] ; then
      log "image is available in bucket, copy it onto worker"
      rclone copy "$image_bucket_path" "./"
      log "image has been copied on worker, load it into docker"
      cat "./$image_tar_file" \
        | docker load
      log "image has been loaded into docker, remove tar archive"
      rm "./$image_tar_file"
    else
      log "image is not available in local bucket"
      log "Error: can't figure how to make $docker_image available locally in docker"
      exit $external_error_status_code
    fi  
  fi
fi

log "done"