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
bash "$here/init_backups.sh"

export CSI_ROOT_DIR=/opt/csi
if [ -d "$CSI_ROOT_DIR/database" ]
then
  log "CSI database directory exists"
else
  log "create CSI root directory"
  mkdir -p "$CSI_ROOT_DIR"
fi

assert_env_var_existence CSI_ROOT_DIR
assert_directory_existence "$CSI_ROOT_DIR"

assert_file_existence "$here/postgrest.config"
assert_file_existence "$here/postgrest.service"
assert_file_existence "$here/init_backups.sh"

if [ $at_least_one_assert_failed = true ]
then
  log "[asserts error] at least one assert failed (see previous assert log messages)"
  exit 1
else
  log "[asserts ok] all asserts are ok"
fi


#-------------------------------------------------------------------------

log "get arguments"
if [ "$#" -ne 1 ]; then
  log "Error: wrong number of arguments"
  log "usage: $0 package_tag"
  exit 1
fi
package_tag=$1

log "start the database instance initialization"

"$here/get_component_package.sh" database "$package_tag"

if [ -d "$CSI_ROOT_DIR/database" ]
then
  log "CSI database directory exists"
  log "this means the volume for this instance has already been initialized"
  log "so there is nothing to do for the initialisation of the instance"
else
  package_base_name="database_${package_tag}"
  component_directory="$CSI_ROOT_DIR/components"

  # Copy the database files from the downloaded directory to the local destination
  # location.
  cp -rp "$component_directory/$package_base_name" "$CSI_ROOT_DIR/database"

  # Disable some warning and questions from apt-get
  export DEBIAN_FRONTEND=noninteractive

  log "apt-get update"
  apt-get update

  log "install some general dependencies"
  apt install --yes wget

  log "install PostgreSQL"
  # Prepare apt get to download any PostgreSQL version.
  echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" >> /etc/apt/sources.list.d/pgdg.list
  wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
  apt-get update

  # Actually install PostgreSQL
  apt install --yes postgresql-12 postgresql-contrib-12

  log "install PostgREST"
  mkdir -p postgrest_install_tmp_dir
  cd postgrest_install_tmp_dir
  wget https://github.com/PostgREST/postgrest/releases/download/v6.0.1/postgrest-v6.0.1-linux-x64-static.tar.xz
  tar xf postgrest-v6.0.1-linux-x64-static.tar.xz
  cp -p result/bin/postgrest /bin/
  cd ..
  rm -fr postgrest_install_tmp_dir

  log "copy the script to clear and init database"
  cp "$here/clear_and_init_database.sh" $CSI_ROOT_DIR/database

  log "copy our PostgREST configuration files"
  mkdir -p /etc/postgrest
  cp "$here/postgrest.config" /etc/postgrest/config
  cp "$here/postgrest.service" /etc/systemd/system/postgrest.service

  log "open in the firewall the port used by PostgREST"
  ufw allow 3000

  log "init database"
  sudo -u postgres psql -f "$CSI_ROOT_DIR/database/init_database.sql"

  log "configure PostgREST service"
  systemctl enable postgrest
  systemctl start postgrest

  log "Tune PostgreSQL 12 memory"
  total_mem=$(free -m | grep ^Mem | awk '{print $2}')
  # shared_buffers = 25% TOTAL_RAM
  sed -i "s/^[#\s]*shared_buffers\s=.*/shared_buffers = $((total_mem/4))MB/" /etc/postgresql/12/main/postgresql.conf
  # max_connections = 100 is the default value, but we use it below to compute work_mem
  # so we want to be sure of its value
  sed -i "s/^[#\s]*max_connections\s=.*/max_connections = 100/" /etc/postgresql/12/main/postgresql.conf
  # work_mem = 25% TOTAL_RAM / max_connections
  sed -i "s/^[#\s]*work_mem\s=.*/work_mem = $((total_mem/4/100))MB/" /etc/postgresql/12/main/postgresql.conf
  # maintenance_work_mem = 5% TOTAL_RAM
  sed -i "s/^[#\s]*maintenance_work_mem\s=.*/maintenance_work_mem = $((total_mem/20))MB/" /etc/postgresql/12/main/postgresql.conf
  # effective_cache_size = 50% TOTAL_RAM
  sed -i "s/^[#\s]*effective_cache_size\s=.*/effective_cache_size = $((total_mem/2))MB/" /etc/postgresql/12/main/postgresql.conf

  log "Restart PostgreSQL 12"
  systemctl try-restart postgresql@12-main.service
fi

database_patch_file="$CSI_ROOT_DIR/database_patch.sql"
if [ -f "$database_patch_file" ]
then
  log "there is a SQL patch for the database"

  log "temporarily stop the HTTP API service"
  systemctl stop postgrest

  log "apply the patch"
  sudo -u postgres psql -f "$database_patch_file"

  log "restart the HTTP API service"
  systemctl start postgrest

  log "archive the patch file"
  patch_directory="$CSI_ROOT_DIR/database/patches/$(date +%Y%m%d_%H%M%S)"
  mkdir -p "$patch_directory"
  mv "$database_patch_file" "$patch_directory"
fi

log "install node_exporter"
node_exporter_version="1.0.1"
curl --silent -LO https://github.com/prometheus/node_exporter/releases/download/v${node_exporter_version}/node_exporter-${node_exporter_version}.linux-amd64.tar.gz
tar -xvf node_exporter-${node_exporter_version}.linux-amd64.tar.gz
chown -R root:root node_exporter-${node_exporter_version}.linux-amd64
mv node_exporter-${node_exporter_version}.linux-amd64/node_exporter  /usr/local/bin/
rm -R node_exporter-${node_exporter_version}.linux-amd64
rm node_exporter-${node_exporter_version}.linux-amd64.tar.gz
log "run node_exporter"
useradd -rs /bin/false node_exporter
cp "$here/node_exporter.service" /etc/systemd/system
systemctl daemon-reload
systemctl enable node_exporter
systemctl start node_exporter

# A key to emit in the log so that is can be searched by any one that want to
# automatically check if the script finished with success.
init_success_key="init_instance_finished_with_success"
log "$init_success_key"

log "done"
