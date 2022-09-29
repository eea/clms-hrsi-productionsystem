#!/bin/bash

# Be sure we fail on error and output debugging information
set -e
trap 'echo "$0: error on line $LINENO"' ERR

here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


#-------------------------------------------------------------------------

tool=$( basename $0 )
log () {
  message=$1
  datestring=$( date +'%Y-%m-%d %H:%M:%S' )
  echo -e "[$tool] [$datestring] $message"
}

at_least_one_assert_failed=false

assert_file_existence () {
  file=$1
  if [ ! -f "$file" ]
  then
    log "[assert fails] missing expected file: $file"
    at_least_one_assert_failed=true
  else
    log "[assert ok] expected file exists: $file"
  fi
}

assert_directory_existence () {
  directory=$1
  if [ ! -d "$directory" ]
  then
    log "[assert fails] missing expected directory: $directory"
    at_least_one_assert_failed=true
  else
    log "[assert ok] expected directory exists: $directory"
  fi
}

assert_env_var_existence () {
  env_var_name=$1
  if [ ! -v "$env_var_name" ]; then
    log "[assert fails] missing expected environment variable: $env_var_name"
    at_least_one_assert_failed=true
  else
    log "[assert ok] expected environment variable exists: $env_var_name"
  fi
}

#-------------------------------------------------------------------------

instances=(
  dashboard
  database
  nomad_server
  orchestrator
  worker
)

#-------------------------------------------------------------------------

log "get arguments"
if [ "$#" -ne 1 ]; then
  log "Error: wrong number of arguments"
  log "usage: $0 package_tag"
  exit 1
fi
package_tag=$1

# Check some preconditions to fulfill before running this script (like the
# existence of some environment variables).

log "check script's preconditions"

source "$here/../../config/main.env"
assert_env_var_existence CSI_BUCKET_NAME_INFRA

for instance in "${instances[@]}"; do
  package_file_name="${instance}_instance_init_${package_tag}.tgz"
  assert_file_existence "$here/packages/$package_file_name"
done

if [ $at_least_one_assert_failed = true ]
then
  log "[asserts error] at least one assert failed (see previous assert log messages)"
  exit 1
else
  log "[asserts ok] all asserts are ok"
fi

log "script's preconditions are OK"

#-------------------------------------------------------------------------

log "start pushing the instance init packages"

rclone_remote_name=csi
infra_bucket=$CSI_BUCKET_NAME_INFRA

log "first check the rclone config..."
if rclone listremotes | grep -q "^$rclone_remote_name:$" ; then
  log "found the rclone remote storage which name is $rclone_remote_name "
else
  log "Error: can't find the rclone remote storage which name is $rclone_remote_name"
  log "please check this remote is properly configured in the rclone config file."
  # Show the default config file for rclone
  rclone config file
  exit 1
fi

if rclone lsd "$rclone_remote_name:$infra_bucket" &> /dev/null ; then
  log "bucket $infra_bucket found on remote $rclone_remote_name"
else
  log "Error: can't find bucket $infra_bucket on remote $rclone_remote_name"
  exit 1
fi

log "rclone config is OK"

log "loop on the instances for which we want to upload the init package"
for instance in "${instances[@]}"; do
  log "upload instance init package for '$instance'"
  package_file_name="${instance}_instance_init_${package_tag}.tgz"
  rclone copy \
   "$here/packages/$package_file_name" \
   "$rclone_remote_name:$infra_bucket/instance_init_packages/$package_tag"
  log "the package is uploaded"
done

log "all instances init packages are uploaded"

log "done"