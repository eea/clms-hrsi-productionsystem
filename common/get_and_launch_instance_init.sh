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

export CSI_ROOT_DIR=/opt/csi

assert_file_existence "$CSI_ROOT_DIR/config/main.env"
source "$CSI_ROOT_DIR/config/main.env"

assert_env_var_existence CSI_BUCKET_NAME_INFRA
export CSI_BUCKET_NAME_INFRA

if [ $at_least_one_assert_failed = true ]
then
  log "[asserts error] at least one assert failed (see previous assert log messages)"
  exit 1
else
  log "[asserts ok] all asserts are ok"
fi

#-------------------------------------------------------------------------

log "start getting and executing package for instance initialisation"

bucket="$CSI_BUCKET_NAME_INFRA"

log "get arguments"
if [ "$#" -ne 2 ]; then
  log "Error: wrong number of arguments"
  log "usage:"
  log "  $0 instance_name package_tag"
  exit 1
fi

instance_name=$1
package_tag=$2

package_file_name="${instance_name}_instance_init_${package_tag}.tgz"

# Package path. Example:
#   foo/instance_init_packages/database_instance_init.tgz
package_bucket_path="$bucket/instance_init_packages/$package_tag/$package_file_name"

# Temporary working directory
mkdir -p "$CSI_ROOT_DIR/init_instance"
cd "$CSI_ROOT_DIR/init_instance"

# TODO implement all the rclone checks like in "ensure_docker_image.sh".
log "get the package from the bucket ($package_bucket_path)..."
if [[ $(rclone lsf "foo$package_bucket_path") = "$package_file_name" ]] ; then
  log "package is available in bucket, download it"
  rclone copy "foo$package_bucket_path" .
else
  log "Error: package is not available in bucket"
  exit 1
fi  

log "extract the package"
tar xf "$package_file_name"

# TODO fail early if the extension is not .tgz

# Get the package without the .tgz extension (i.e.
# /path/to/my_package.tgz -> my_package)
package_name=$( basename "$package_bucket_path" .tgz )

log "launch the instance initialisation"
"./$package_name/init_instance.sh" "$package_tag"

log "done"