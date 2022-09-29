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

if [ "$#" -ne 1 ]; then
  log "Error: wrong number of arguments."
  log "usage: $0 terraform_target"
  exit 1
fi
target=$1

log "go to terraform directory and ask for env destruction..."

cd "$here/terraform"
terraform \
  destroy \
  -target "$target" \
  -parallelism=2 \
  -var='csi_packages_tag="no_useful_during_destroy"' \
  -var-file="$here/prod_env_configuration.tfvars" \
  -var-file="$here/prod_env_secrets.tfvars"
