#!/bin/bash

# Be sure we fail on error and output debugging information
set -e
set -o pipefail
trap 'log "error on line $LINENO" ; set_job_status "$job_id" failed' ERR

here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

#-------------------------------------------------------------------------

# Define some utility functions

# Utility to fail the script if one environment variable is missing.
error_on_missing_env () {
  env_var_name=$1
  if [ ! -v "$env_var_name" ]; then
    echo "$0: the required environment variable '$env_var_name' is missing"
    exit 100
  fi
}

#-------------------------------------------------------------------------
# Be sure expected parameters are set:
error_on_missing_env COSIMS_DB_HTTP_API_BASE_URL
error_on_missing_env components_out
error_on_missing_env CSI_SIP_DATA_BUCKET

# Everything is ok, proceed.

#-------------------------------------------------------------------------

pytest components/*/tests/unit
