#!/usr/bin/env bash

# Be sure we fail on error and output debugging information
set -e
trap 'echo "$0: error on line $LINENO"' ERR

here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Utility to fail the script if one environment variable is missing.
error_on_missing_env () {
  env_var_name=$1
  if [ ! -v "$env_var_name" ]; then
    echo "the required environment variable '$env_var_name' is missing"
    exit 1
  fi
}

#-------------------------------------------------------------------------

error_on_missing_env COSIMS_DB_HTTP_API_BASE_URL

if [ "$#" -ne 1 ]; then
  echo "wrong number of arguments"
  echo "usage: $0 job_id"
  exit 1
fi

export job_id=$1

export PYTHONPATH=$( realpath "$here/../../.." )

# Get docker image name to be used from database's 'system_parameters' table
system_parameters=$(
  curl \
    --silent \
    --header "Accept: application/vnd.pgrst.object+json" \
    "$COSIMS_DB_HTTP_API_BASE_URL/system_parameters?id=eq.1"
)
docker_image_for_rlies1s2_processing=$( echo "$system_parameters" | jq -r '.docker_image_for_rliepart2_processing' )

echo "make sure the docker image for test_job processing is available"
"$here/ensure_docker_image.sh" "$docker_image_for_rlies1s2_processing"


python3 -c "
import components.worker.python.run_rlies1s2_worker as run_rlies1s2_worker
run_rlies1s2_worker.main($job_id)
"