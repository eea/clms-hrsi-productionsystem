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

# Everything is ok, proceed.

#-------------------------------------------------------------------------

cd cosims

apt-get update && apt-get install -y --fix-missing gdal-bin python3-gdal python3-distutils
export PYTHONPATH="${PYTHONPATH}:/home/eouser/.local/lib/python3.7/site-packages:/usr/local/lib/python3.7/dist-packages:/usr/lib/python3/dist-packages:/usr/local/lib/python3.7/:/usr/local/lib/python3.7/site-packages:/usr/local/lib/python3.7/lib-dynload:/usr/local/lib/python37.zip"

components/common/docker/pip_csi_python.sh

python3 -m pip install -U pytest

pytest components/*/tests/functional
