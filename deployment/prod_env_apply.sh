#!/usr/bin/env bash

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

source "$here/../config/main.env"
assert_env_var_existence CSI_BUCKET_NAME_INFRA

if [ $at_least_one_assert_failed = true ]
then
  log "[asserts error] at least one assert failed (see previous assert log messages)"
  exit 1
else
  log "[asserts ok] all asserts are ok"
fi

log "script's preconditions are OK"

#-------------------------------------------------------------------------

EXPECTED_OS_PROJECT_ID=hidden_value
if [ "$OS_PROJECT_ID" != "$EXPECTED_OS_PROJECT_ID" ] ; then
  log "Error: bad OpenStack project (OS_PROJECT_ID must be $EXPECTED_OS_PROJECT_ID)"
  log "The configuration has been set especially for this project. Please"
  log "set all the OpenStack env vars with this projevt (usually"
  log "executing a the appropriate .sh file and typing the corresponding"
  log "password)."
  exit 1
fi

#-------------------------------------------------------------------------

if [ "$#" -eq 2 ]; then
  log "use the second argument as the tag for all packages"
  packages_tag=$2
elif [ "$#" -eq 1 ]; then
  log "no tag argument given for this script: we use a tag build from git hash"
  packages_tag="git-hash-$(git rev-parse --short=8 HEAD)"
  # Reflect it in the tag if there are some uncommited changes in the git
  # repository.
  if ! git diff-index --quiet HEAD --; then
    packages_tag="$packages_tag-changed"
  fi
else
  log "Error: wrong number of arguments."
  log "usage: $0 terraform_target [packages_tag]"
  log "For example, if you want to deploy the core of the system with the "
  log "default tag (which is based on the last git commit hash) use this"
  log "command:"
  log "  $0 module.core"
  exit 1
fi

target="$1"

log "use the following tag for the packages: $packages_tag"

rclone_remote_name=csi
infra_bucket=$CSI_BUCKET_NAME_INFRA

if rclone lsd "$rclone_remote_name:$infra_bucket" &> /dev/null ; then
  log "bucket $infra_bucket found on remote $rclone_remote_name"
else
  log "Error: can't find bucket $infra_bucket on remote $rclone_remote_name"
  exit 1
fi

if [[ $(rclone lsf "$rclone_remote_name:$infra_bucket/instance_init_packages/$packages_tag") ]]; then
  log "init packages with tag $packages_tag found on the $infra_bucket bucket"
else
  log "Warning:"
  log "  can't find the init packages with tag $packages_tag on the $infra_bucket bucket"
  log "  if this Terraform apply starts some instances their initialiszation will most certainly fail"
  log "  only continue if you know what your are doing"
fi

read -p "do you want to proceed? Only 'yes' will be accepted to approve: " answer
if [[ $answer != yes ]]
then
  log "Terraform apply aborted"
  exit 0
fi

log "go to terraform directory and ask for env creation..."

cd "$here/terraform"
terraform \
  apply \
  -target "$target" \
  -parallelism=2 \
  -input=false \
  -var='csi_packages_tag="'$packages_tag'"' \
  -var-file="$here/prod_env_configuration.tfvars" \
  -var-file="$here/prod_env_secrets.tfvars"
