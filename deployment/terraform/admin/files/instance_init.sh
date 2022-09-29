#!/bin/bash

# Be sure we fail on error and output debugging information
set -e
trap 'echo "$0: error on line $LINENO"' ERR

here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


#-------------------------------------------------------------------------

tool=$0
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

#-------------------------------------------------------------------------

# Check some preconditions to fulfill before running this script (like the
# existence of some environment variables).

# log "check script's preconditions"

# if [ $at_least_one_assert_failed = true ]
# then
#   log "[asserts error] at least one assert failed (see previous assert log messages)"
#   exit 1
# else
#   log "[asserts ok] all asserts are ok"
# fi

# log "script's preconditions are OK"

#-------------------------------------------------------------------------

log "start admin server instance init script"

apt-get update

log "install some dependencies"
apt install --yes \
  unzip \
  curl \
  jq

log "install rclone"
curl --silent --remote-name  https://downloads.rclone.org/v1.50.2/rclone-v1.50.2-linux-amd64.zip                         
unzip rclone-v1.50.2-linux-amd64.zip
chown root:root rclone-v1.50.2-linux-amd64/rclone
mv rclone-v1.50.2-linux-amd64/rclone /usr/local/bin/
rm rclone-v1.50.2-linux-amd64.zip
rm -rf rclone-v1.50.2-linux-amd64

log "install openstack command line tool"
apt install --yes python3-openstackclient

log "install docker"
apt install --yes docker.io

#TODO Check weither  it is useful
log "install nomad"
nomad_version="0.11.1"
curl --silent --remote-name https://releases.hashicorp.com/nomad/${nomad_version}/nomad_${nomad_version}_linux_amd64.zip
unzip nomad_${nomad_version}_linux_amd64.zip
chown root:root nomad
mv nomad /usr/local/bin/
rm nomad_${nomad_version}_linux_amd64.zip

log "install terraform"
terraform_version="0.12.24"
curl --silent --remote-name https://releases.hashicorp.com/terraform/${terraform_version}/terraform_${terraform_version}_linux_amd64.zip
unzip terraform_${terraform_version}_linux_amd64.zip
chown root:root terraform
mv terraform /usr/local/bin/
rm terraform_${terraform_version}_linux_amd64.zip

log "install packer"
packer_version="1.5.1"
curl --silent --remote-name https://releases.hashicorp.com/packer/${packer_version}/packer_${packer_version}_linux_amd64.zip
unzip packer_${packer_version}_linux_amd64.zip
chown root:root packer
mv packer /usr/local/bin/
rm packer_${packer_version}_linux_amd64.zip

log "done"