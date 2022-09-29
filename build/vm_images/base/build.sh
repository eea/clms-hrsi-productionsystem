#!/bin/bash

# Be sure we fail on error and output debugging information
set -e
trap 'echo "$0: error on line $LINENO"' ERR

here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


#-------------------------------------------------------------------------

tool=$0
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

assert_env_var_existence () {
  env_var_name=$1
  if [ ! -v "$env_var_name" ]; then
    log "[assert fails] missing expected environment variable: $env_var_name"
    at_least_one_assert_failed=true
  else
    log "[assert ok] expected environment variable exists: $env_var_name"
  fi
}

assert_command_existence () {
  command=$1
  if ! command -v "$command" >/dev/null 2>&1 ; then
    log "[assert fails] missing '$command' executable, be sure it is in your PATH"
    at_least_one_assert_failed=true
  else
    log "[assert ok] expected executable '$command' found"
  fi
}

#-------------------------------------------------------------------------

# Check some preconditions to fulfill before running this script (like the
# existence of some environment variables).

log "check script's preconditions"

assert_env_var_existence OS_USERNAME
assert_env_var_existence OS_PASSWORD

assert_file_existence "$here/../../../config/main.env"
source "$here/../../../config/main.env"
assert_env_var_existence CSI_INTERNAL_EC2_CREDENTIALS_ACCESS_KEY
assert_env_var_existence CSI_INTERNAL_EC2_CREDENTIALS_SECRET_KEY
assert_env_var_existence CSI_PRIVATE_NETWORK_OPENSTACK_NAME

assert_file_existence vm_setup.sh
assert_file_existence image_build.json

assert_file_existence "$here/../../../common/get_openstack_ec2_credentials.sh"
assert_file_existence "$here/../../../common/rclone_template.conf"

assert_command_existence packer
assert_command_existence git
assert_command_existence envsubst
assert_command_existence openstack

if [ $at_least_one_assert_failed = true ]
then
  log "[asserts error] at least one assert failed (see previous assert log messages)"
  exit 1
else
  log "[asserts ok] all asserts are ok"
fi

log "script's preconditions are OK"

#-------------------------------------------------------------------------

log "start building the base image..."

export CSI_INTERNAL_EC2_CREDENTIALS_ACCESS_KEY
export CSI_INTERNAL_EC2_CREDENTIALS_SECRET_KEY

mkdir -p tmp
envsubst \
  '$CSI_INTERNAL_EC2_CREDENTIALS_ACCESS_KEY $CSI_INTERNAL_EC2_CREDENTIALS_SECRET_KEY' \
  < "$here/../../../common/rclone_template.conf" \
  > tmp/rclone.conf

# Use the git hash as a tag for the resulting image.
image_tag=git-$(git rev-parse --short=8 HEAD)
# Reflect it in the tag if there are some uncommited changes in the git
# repository.
if ! git diff-index --quiet HEAD --; then
  image_tag="$image_tag-changed"
fi

# Ask OpenStack what is the ID of the sub network that Packer will use during
# the build of the image.
network_id=$(
  openstack \
    network show \
    --column id \
    --format value \
    "$CSI_PRIVATE_NETWORK_OPENSTACK_NAME"
)

packer build \
  -var image_tag="$image_tag" \
  -var network_id="$network_id" \
  image_build.json

log "clean files"
rm -rf tmp

log "done"