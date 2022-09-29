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

assert_env_var_existence CSI_NOMAD_SERVER_IP
assert_file_existence "$here/csi_nomad.service"
assert_file_existence "$here/envsubst/nomad_orchestrator_client.hcl"

if [ $at_least_one_assert_failed = true ]
then
  log "[asserts error] at least one assert failed (see previous assert log messages)"
  exit 1
else
  log "[asserts ok] all asserts are ok"
fi


#-------------------------------------------------------------------------

log "start the Nomad server instance initialization"

# Disable some warning and questions from apt-get
export DEBIAN_FRONTEND=noninteractive

log "apt-get update"
apt-get update

log "install some general dependencies"
apt install --yes \
  unzip \
  curl \
  gettext-base

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
  < "$here/envsubst/nomad_orchestrator_client.hcl" \
  > "$here/nomad_orchestrator_client.hcl"

log "copy our Nomad configuration files"
cp "$here/csi_nomad.service" /etc/systemd/system
cp "$here/nomad_orchestrator_client.hcl" /etc/nomad.d

log "Open in the firewall the port used by Nomad"
ufw allow 4646
ufw allow 4647

log "configure Nomad services"
systemctl enable csi_nomad
systemctl start csi_nomad

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
