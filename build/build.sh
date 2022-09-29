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
  database
  worker
)
components_dir=$( realpath "$here/../../components" )

#-------------------------------------------------------------------------

# Check some preconditions to fulfill before running this script (like the
# existence of some environment variables).

log "check script's preconditions"

assert_file_existence "$here/components_packages/build.sh"
assert_file_existence "$here/instance_init_packages/build.sh"
assert_file_existence "$here/instance_init_packages/push.sh"

if [ $at_least_one_assert_failed = true ]
then
  log "[asserts error] at least one assert failed (see previous assert log messages)"
  exit 1
else
  log "[asserts ok] all asserts are ok"
fi

log "script's preconditions are OK"

#-------------------------------------------------------------------------

if [ "$#" -eq 1 ]; then
  log "use the first argument as the tag for all packages"
  packages_tag=$1
elif [ "$#" -eq 0 ]; then
  log "no argument given for this script: we use a tag build from git hash"
  packages_tag="git-hash-$(git rev-parse --short=8 HEAD)"
  # Reflect it in the tag if there are some uncommited changes in the git
  # repository.
  if ! git diff-index --quiet HEAD --; then
    packages_tag="$packages_tag-changed"
  fi
else
  log "Error: wrong number of arguments. Please provid one or zero argument."
  log "usage: $0 [packages_tag]"
  exit 1
fi
log "use the following tag for all packages: $packages_tag"

log "start preparing packages for deployement"
log "create the packages for the components"
"$here/components_packages/build.sh" "$packages_tag"
"$here/components_packages/push.sh" "$packages_tag"

log "create the packages for the instances initialization with tag $packages_tag"
"$here/instance_init_packages/build.sh" "$packages_tag"
"$here/instance_init_packages/push.sh" "$packages_tag"

log "all the packages are ready and uploaded for tag $packages_tag"

log "done"
