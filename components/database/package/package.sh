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

assert_file_existence "$here/../init_database.sql"

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

log "start building the database package..."

package_name="database_$package_tag"

log "create working directory ($package_name)"
mkdir "$package_name"

log "copy the database package files"
cp "$here/../init_database.sql" "$package_name"

log "create the archive"
tar czf "$package_name.tgz" "$package_name"

log "clean working directory"
rm -r "$package_name"

log "done"