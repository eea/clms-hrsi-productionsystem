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

if [ "$#" -ne 0 ]; then
  echo "this tool takes no argument"
  echo "usage: $0"
  exit 1
fi

curl \
    --silent \
    "$COSIMS_DB_HTTP_API_BASE_URL/system_parameters" \
    --header "Content-Type: application/json" \
    --header "Accept: application/json"
