#!/usr/bin/env bash

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

#-------------------------------------------------------------------------

log "go to terraform directory"
cd "$here/terraform"

log "get the SSH private key file from the Terraform state for the admin instance"
terraform output -json admin_external_private_key | jq -r '' > admin_id_rsa

log "set the appropriate access rights for the private key file"
chmod 600 admin_id_rsa

log "get the IP address of the admin instance"
ADMIN_IP_ADDRESS=$(terraform output -json admin_ip_address | jq -r '.')

log "actually connect to the admin instance with SSH"
ssh -i admin_id_rsa eouser@$ADMIN_IP_ADDRESS