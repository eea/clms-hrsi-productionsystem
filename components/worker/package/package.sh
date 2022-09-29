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

# Check some preconditions to fulfill before running this script (like the
# existence of some environment variables).

log "check script's preconditions"

component_directories=(
  "worker/python"
  "worker/src"
  "common/python"
  "job_creation/config"
  "job_creation/geometry"
)
for component_directory in "${component_directories[@]}"; do
  assert_directory_existence "$here/../../../components/$component_directory"
done

assert_file_existence "$here/install.sh"

if [ $at_least_one_assert_failed = true ]
then
  log "[asserts error] at least one assert failed (see previous assert log messages)"
  exit 1
else
  log "[asserts ok] all asserts are ok"
fi

log "script's preconditions are OK"

#-------------------------------------------------------------------------

log "get arguments"
if [ "$#" -ne 1 ]; then
  log "Error: wrong number of arguments"
  log "usage: $0 package_tag"
  exit 1
fi
package_tag=$1

log "start building the worker package..."

package_name="worker_$package_tag"

log "create working directory ($package_name)"
mkdir -p "$package_name"

log "copy the worker package files"
for component_directory in "${component_directories[@]}"; do
  # Example:
  #   component_directory=common/python
  #   parent_dir=common
  parent_dir=$( dirname "$component_directory" )
  # Example:
  #   create dir ./worker/componentes/common
  mkdir -p "$package_name/components/$parent_dir"
  # Example:
  #   copy content of ../../components/common/python
  #   into ./worker/componentes/common
  cp -r \
    "$here/../../../components/$component_directory" \
    "$package_name/components/$parent_dir"
done

log "remove .pyc and pyo files, then __pycache__ directories"
find "$package_name" \
  -type f -name '*.py[co]' -delete \
  -o \
  -type d -name __pycache__ -delete

log "copy the install script for the package"
cp "$here/install.sh" "$package_name"

log "create the archive"
tar czf "$package_name.tgz" "$package_name"

log "clean working directory"
rm -r "$package_name"

log "done"