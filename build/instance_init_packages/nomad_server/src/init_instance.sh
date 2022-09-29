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

nomad_jobs=(
  job_creation
  job_configuration
  job_execution
  job_publication
  monitor
  worker_pool_management
  si_processing
  test_job_processing
  rlies1_processing
  rlies1s2_processing
  gfsc_processing
  ws_processing
)

assert_env_var_existence CSI_HTTP_API_INSTANCE_IP
assert_env_var_existence OS_PASSWORD
assert_env_var_existence CSI_PRODUCT_PUBLICATION_ENDPOINT_PASSWORD
assert_env_var_existence CSI_PRODUCTS_BUCKET_EC2_CREDENTIALS_SECRET_KEY
assert_env_var_existence CSI_SCIHUB_ACCOUNT_PASSWORD
assert_env_var_existence GITLAB_TOKEN_PASSWORD

assert_file_existence "$here/get_openstack_ec2_credentials.sh"

assert_file_existence "$here/config/main.env"
source "$here/config/main.env"
assert_env_var_existence CSI_ROOT_DIR

assert_file_existence "$CSI_ROOT_DIR/config/openstack.sh"

assert_env_var_existence CSI_INTERNAL_EC2_CREDENTIALS_ACCESS_KEY
assert_env_var_existence CSI_INTERNAL_EC2_CREDENTIALS_SECRET_KEY
assert_env_var_existence CSI_PRODUCTS_BUCKET_EC2_CREDENTIALS_ACCESS_KEY

assert_file_existence "$here/csi_nomad.service"
assert_file_existence "$here/nomad_server.hcl"

for nomad_job in "${nomad_jobs[@]}"; do
  assert_file_existence "$here/envsubst/$nomad_job.nomad"
done


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
  gettext-base \
  python3-openstackclient

log "install Nomad"
nomad_version="0.11.1"
curl --silent --remote-name https://releases.hashicorp.com/nomad/${nomad_version}/nomad_${nomad_version}_linux_amd64.zip
unzip nomad_${nomad_version}_linux_amd64.zip
chown root:root nomad
mv nomad /usr/local/bin/
rm nomad_${nomad_version}_linux_amd64.zip

mkdir --parents /opt/nomad

log "copy our Nomad configuration files"
cp "$here/csi_nomad.service" /etc/systemd/system
cp "$here/nomad_server.hcl" /etc/nomad.d

log "Open in the firewall the port used by Nomad"
ufw allow 4646
ufw allow 4647

log "configure Nomad services"
systemctl enable csi_nomad
systemctl start csi_nomad

log "wait a bit to be sure Nomad server is started..."
sleep 10

log "Get the IP address of this machine to use in config.env file"
nomad_server_ip=$( hostname --ip-address | awk '{print $1}' )

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
  echo "CSI_NOMAD_SERVER_IP=$nomad_server_ip"
  echo "NOMAD_ADDR=http://$nomad_server_ip:4646"
) >> "$CSI_ROOT_DIR/config/config.env"

log "prepare file with secrets env vars for OpenStack"
(
  echo "OS_PASSWORD=\"${OS_PASSWORD}\""
  echo "CSI_PRODUCT_PUBLICATION_ENDPOINT_PASSWORD=\"${CSI_PRODUCT_PUBLICATION_ENDPOINT_PASSWORD}\""
) > "$CSI_ROOT_DIR/config/secrets.env"

log "extend file with secrets env vars for access to internal buckets"

source "$CSI_ROOT_DIR/config/openstack.sh"

log "add OpenStack env vars"
(
   # Be sure there is a newline at the end of the previous content.
  echo ""
  echo "OS_USERNAME=$OS_USERNAME"
  echo "OS_AUTH_URL=$OS_AUTH_URL"
  echo "OS_PROJECT_ID=$OS_PROJECT_ID"
  echo "OS_PROJECT_NAME=$OS_PROJECT_NAME"
  echo "OS_USER_DOMAIN_NAME=$OS_USER_DOMAIN_NAME"
  echo "OS_PROJECT_DOMAIN_ID=$OS_PROJECT_DOMAIN_ID"
  echo "OS_REGION_NAME=$OS_REGION_NAME"
  echo "OS_INTERFACE=$OS_INTERFACE"
  echo "OS_IDENTITY_API_VERSION=$OS_IDENTITY_API_VERSION"
) >> "$CSI_ROOT_DIR/config/config.env"

(
  echo "CSI_INTERNAL_EC2_CREDENTIALS_ACCESS_KEY=$CSI_INTERNAL_EC2_CREDENTIALS_ACCESS_KEY"
  echo "CSI_INTERNAL_EC2_CREDENTIALS_SECRET_KEY=$CSI_INTERNAL_EC2_CREDENTIALS_SECRET_KEY"
) >> "$CSI_ROOT_DIR/config/secrets.env"

(
  echo "CSI_PRODUCTS_BUCKET_EC2_CREDENTIALS_ACCESS_KEY=$CSI_PRODUCTS_BUCKET_EC2_CREDENTIALS_ACCESS_KEY"
  echo "CSI_PRODUCTS_BUCKET_EC2_CREDENTIALS_SECRET_KEY=$CSI_PRODUCTS_BUCKET_EC2_CREDENTIALS_SECRET_KEY"
) >> "$CSI_ROOT_DIR/config/secrets.env"

(
  echo "CSI_SCIHUB_ACCOUNT_PASSWORD=$CSI_SCIHUB_ACCOUNT_PASSWORD"
) >> "$CSI_ROOT_DIR/config/secrets.env"

(
  echo "GITLAB_TOKEN_PASSWORD=$GITLAB_TOKEN_PASSWORD"
) >> "$CSI_ROOT_DIR/config/secrets.env"

export CSI_CONFIG_ENV_FILE_CONTENT=$( cat "$CSI_ROOT_DIR/config/config.env" )
export CSI_SECRETS_ENV_FILE_CONTENT=$( cat "$CSI_ROOT_DIR/config/secrets.env" )
export CSI_ROOT_DIR
for nomad_job in "${nomad_jobs[@]}"; do
  log "prepare Nomad job file for $nomad_job"
  # shellcheck disable=SC2016
  # shellcheck disable=SC2016
  envsubst \
    '$CSI_ROOT_DIR $CSI_CONFIG_ENV_FILE_CONTENT $CSI_SECRETS_ENV_FILE_CONTENT $GITLAB_TOKEN_PASSWORD' \
    < "$here/envsubst/$nomad_job.nomad" \
    > "$here/$nomad_job.nomad"
  log "add the Nomad job $nomad_job"
  nomad run -detach "$here/$nomad_job.nomad"
done

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
