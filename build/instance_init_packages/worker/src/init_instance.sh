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

assert_file_existence "$here/config/main.env"

source "$here/config/main.env"
assert_env_var_existence CSI_ROOT_DIR
mkdir -p "$CSI_ROOT_DIR"

assert_env_var_existence CSI_HTTP_API_INSTANCE_IP
assert_env_var_existence CSI_NOMAD_SERVER_IP
assert_file_existence "$here/csi_nomad.service"
assert_file_existence "$here/envsubst/nomad_worker_client.hcl"


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

log "start the worker instance initialization"

# Disable some warning and questions from apt-get
export DEBIAN_FRONTEND=noninteractive

log "apt-get update"
apt-get update

log "install some general dependencies"
apt install --yes \
  unzip \
  curl \
  gettext-base \
  python-pip
pip install yq

package_base_name="worker_${package_tag}"
package_file_name="${package_base_name}.tgz"

"$here/get_component_package.sh" worker "$package_tag"

package_base_name="worker_${package_tag}"
component_directory="$CSI_ROOT_DIR/components"
ln -s \
  "$CSI_ROOT_DIR/components/$package_base_name" \
  "$CSI_ROOT_DIR/worker"

log "install the worker package"
"$CSI_ROOT_DIR/worker/install.sh"

log "get the docker image name for S&I processing"
# Disable failing this script on error because the database might not been
# responding and we want to proceed in that case.
set +e
(
  # Get docker image name to be used from database's 'system_parameters' table
  system_parameters=$(
    curl \
      --silent \
      --header "Accept: application/vnd.pgrst.object+json" \
      "$COSIMS_DB_HTTP_API_BASE_URL/system_parameters?id=eq.1"
  ) \
  && \
  docker_image_for_si_processing=$( echo "$system_parameters" | jq -r '.docker_image_for_si_processing' )
)
# Get the status of the database interaction
status=$?
# Bring back the normal behavior of failing this script on any command error.
set -e

if [ $status -ne 0 ] ; then
  log "database is not responding, we can't get the docker image name nor"
  log "download it it will be downloaded later during worker execution if"
  log "database responds back at that time in the meantime, we proceed the end"
  log "of worker initialization"
else
  # We were able to retreive the docker image name from the database.
  log "make sure the docker image for S&I processing is available"
  "$CSI_ROOT_DIR/worker/components/worker/src/ensure_docker_image.sh" \
    "$docker_image_for_si_processing"
fi


log "install Nomad"
nomad_version="0.11.1"
curl --silent --remote-name https://releases.hashicorp.com/nomad/${nomad_version}/nomad_${nomad_version}_linux_amd64.zip
unzip nomad_${nomad_version}_linux_amd64.zip
chown root:root nomad
mv nomad /usr/local/bin/
rm nomad_${nomad_version}_linux_amd64.zip

mkdir --parents /opt/nomad

log "substitue environment variables in some files"
envsubst \
  '$CSI_NOMAD_SERVER_IP' \
  < "$here/envsubst/nomad_worker_client.hcl" \
  > "$here/nomad_worker_client.hcl"

log "copy our Nomad configuration files"
cp "$here/csi_nomad.service" /etc/systemd/system
cp "$here/nomad_worker_client.hcl" /etc/nomad.d

log "Open in the firewall the port used by Nomad"
ufw allow 4646
ufw allow 4647

log "configure Nomad services"
systemctl enable csi_nomad
# systemctl start csi_nomad

log "create local directory for CSI project"
mkdir -p "$CSI_ROOT_DIR/config"

log "init config env vars file with main config"
cp "$here/config/main.env" "$CSI_ROOT_DIR/config/config.env"

log "add infra env vars"
(
   # Be sure there is a newline at the end of the previous content.
  echo ""
  echo "CSI_HTTP_API_INSTANCE_IP=$CSI_HTTP_API_INSTANCE_IP"
  echo "CSI_HTTP_API_BASE_URL=http://$CSI_HTTP_API_INSTANCE_IP:3000"
  echo "# Deprecated use of COSIMS_DB_HTTP_API_BASE_URL"
  echo "COSIMS_DB_HTTP_API_BASE_URL=http://$CSI_HTTP_API_INSTANCE_IP:3000"
  echo "CSI_NOMAD_SERVER_IP=$CSI_NOMAD_SERVER_IP"
  echo "NOMAD_ADDR=http://$CSI_NOMAD_SERVER_IP:4646"
) >> "$CSI_ROOT_DIR/config/config.env"


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
