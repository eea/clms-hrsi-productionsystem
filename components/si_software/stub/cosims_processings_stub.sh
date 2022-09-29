#!/bin/bash

# Be sure we fail on error and output debugging information
set -e
trap 'echo "$0: error on line $LINENO"' ERR

tool=$0
log () {
  message=$1

  declare -A style
  style[end]="\e[0m"
  style[green]="\e[32m"
  style[blue]="\e[34m"

  datestring=$( date +'%Y-%m-%d %H:%M:%S' )
  echo -e "[${style[blue]}$tool${style[end]}]\
 [${style[green]}$datestring${style[end]}]\
 $message"
}


stub_data_dir=$1
parameters_file=$2

if [ ! -f "$parameters_file" ]
then
  log "missing parameter file ($parameters_file)"
  exit 1
fi

get_parameter () {
  yq_query=$1
  yq -r "$yq_query" < "$parameters_file"
}

output_dir=$( get_parameter '.output_dir' )
input_dir=$( get_parameter '.maja.l1c_file' )

if [ ! -d "$output_dir" ]
then
  log "missing output directory ($output_dir)"
  exit 2
fi

if [ ! -d "$input_dir" ]
then
  log "missing input directory ($input_dir)"
  exit 3
fi

log "start stub for cosims processings"
sleep 2
cp -r "$stub_data_dir"/outputs/* "$output_dir"

log "done"