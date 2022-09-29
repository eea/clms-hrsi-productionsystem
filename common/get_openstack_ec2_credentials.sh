#!/bin/bash
#
# Script to prepare stuff (files, etc.) for deployment with terraform.
#
# Requires a valid OpenStack configuration (i.e. the environment variables OS_*
# must have been set).

# Be sure we fail on error and output debugging information
set -e
trap 'echo "$0: error on line $LINENO" ; exit 1' ERR

here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


#-------------------------------------------------------------------------
# Define some utility functions

# Utility to fail the script if one environment variable is missing.
error_on_missing_env () {
  env_var_name=$1
  if [ ! -v "$env_var_name" ]; then
    echo "error: the required environment variable '$env_var_name' is missing"
    exit 100
  fi
}

tool=$0
log () {
  message=$1
  datestring=$( date +'%Y-%m-%d %H:%M:%S' )
  echo -e "[$tool] [$datestring] $message"
}

#-------------------------------------------------------------------------

# Check some preconditions to fulfill before running this script (like the
# existence of some environment variables).
error_on_missing_env OS_USERNAME
error_on_missing_env OS_PASSWORD

# TODO fail if dependencies are missing (like openstack)

if [ "$#" -ne 1 ]; then
  echo "wrong number of arguments"
  echo "usage: $0 ec2_access_key"
  exit 1
fi

# Everything is ok, proceed.

access_key=$1
openstack \
  ec2 credentials list \
  --format json \
  | jq -c  '.[] | select( .Access == "'$access_key'")'
