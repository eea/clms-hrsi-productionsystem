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

log "get arguments"
if [[ ("$#" -gt 2) || ("$#" -lt 1) ]]; then
  log "Error: wrong number of arguments"
  log "usage: $0 nomad_job_to_deploy relative_path_to_config_files"
  exit 1
fi
argument_nomad_jobs=$1
# This parameter is the relative path leading to the config and secret env files 
# directory while being located in the CSI_ROOT_DIR one.
# example : 
#   - config.env located under "/opt/csi/test/config/config.env"
#   - CSI_ROOT_DIR="/opt/csi" 
#   => relative_path_to_config_files should be : "test/config"
relative_path_to_config_files=${2:-config}

# Check argument_nomad_jobs value
if [ $argument_nomad_jobs = all ]
then
  log "Deploying ALL the nomad jobs!"
  nomad_jobs=(
    job_creation
    job_configuration
    job_execution
    job_publication
    monitor
    worker_pool_management
    si_processing
    test_job_processing
  )
else
  log "Deploying only '$argument_nomad_jobs' job!"
  nomad_jobs=(
    $argument_nomad_jobs
  )
fi

assert_env_var_existence CSI_ROOT_DIR
assert_env_var_existence NOMAD_ADDR

assert_file_existence "$CSI_ROOT_DIR/$relative_path_to_config_files/config.env"
assert_file_existence "$CSI_ROOT_DIR/$relative_path_to_config_files/secrets.env"

for nomad_job in "${nomad_jobs[@]}"; do
  assert_file_existence "./envsubst/$nomad_job.nomad"
done


if [ $at_least_one_assert_failed = true ]
then
  log "[asserts error] at least one assert failed (see previous assert log messages)"
  exit 1
else
  log "[asserts ok] all asserts are ok"
fi

#-------------------------------------------------------------------------

export CSI_CONFIG_ENV_FILE_CONTENT=$( cat "$CSI_ROOT_DIR/$relative_path_to_config_files/config.env" )
export CSI_SECRETS_ENV_FILE_CONTENT=$( cat "$CSI_ROOT_DIR/$relative_path_to_config_files/secrets.env" )
export CSI_ROOT_DIR
for nomad_job in "${nomad_jobs[@]}"; do
  log "prepare Nomad job file for $nomad_job"
  # shellcheck disable=SC2016
  # shellcheck disable=SC2016
  envsubst \
    '$CSI_ROOT_DIR $CSI_CONFIG_ENV_FILE_CONTENT $CSI_SECRETS_ENV_FILE_CONTENT' \
    < "./envsubst/$nomad_job.nomad" \
    > "$here/$nomad_job.nomad"
  log "add the Nomad job $nomad_job"
  nomad run -detach "$here/$nomad_job.nomad"
done

log "done"
