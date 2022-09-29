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

components=(
  dashboard
  database
  worker
)
components_dir=$( realpath "$here/../../components" )

#-------------------------------------------------------------------------

# Check some preconditions to fulfill before running this script (like the
# existence of some environment variables).

log "check script's preconditions"

assert_directory_existence "$components_dir"

for component in "${components[@]}"; do
  assert_file_existence "$components_dir/$component/package/package.sh"
done

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

log "start preparing the components packages with tag $package_tag"

log "create the directory where the packages will be stored"
rm -fr "$here/packages"
mkdir -p "$here/packages"
cd "$here/packages"

log "loop on the components to package"
for component in "${components[@]}"; do
  log "create the package for component '$component'"
  "$components_dir/$component/package/package.sh" "$package_tag"
  log "the package for component '$component' with tag '$package_tag' is ready"
done

log "the packages for all the components are ready"

log "done"