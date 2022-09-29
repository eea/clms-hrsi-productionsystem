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

assert_file_existence "$here/nginx/sites-available/proxy_ssl"
assert_file_existence "$here/nginx/sites-available/proxy"
assert_file_existence "$here/nginx/snippets/common.conf"
assert_file_existence "$here/nginx/snippets/letsencrypt.conf"
assert_file_existence "$here/nginx/snippets/security.conf"
assert_file_existence "$here/nginx/snippets/ssl.conf"
assert_file_existence "$here/nginx/snippets/options-proxypass.conf"
assert_file_existence "$here/letsencrypt/cosims.ini"
assert_file_existence "$here/letsencrypt/bar.ini"
assert_file_existence "$here/letsencrypt/deploy-hook"

if [ $at_least_one_assert_failed = true ]
then
  log "[asserts error] at least one assert failed (see previous assert log messages)"
  exit 1
else
  log "[asserts ok] all asserts are ok"
fi


#-------------------------------------------------------------------------

log "start the dashboard proxy instance initialization"

# Disable some warning and questions from apt-get
export DEBIAN_FRONTEND=noninteractive

log "apt-get update"
apt-get update

log "Install web server"
apt-get install --yes --no-install-recommends nginx-light apache2-utils

log "Generate web server password file"
htpasswd -bmc /etc/nginx/.htpasswd api_user Ait6vieyeipha0eeZais8az6Yie9umeeHavokaephe4phohdaBaidoshiof0yeoR

log "copy web server config files"
cp "$here/nginx/sites-available/proxy_ssl" "/etc/nginx/sites-available/proxy_ssl"
cp "$here/nginx/sites-available/proxy" "/etc/nginx/sites-available/proxy"
cp "$here/nginx/snippets/common.conf" "/etc/nginx/snippets/common.conf"
cp "$here/nginx/snippets/letsencrypt.conf" "/etc/nginx/snippets/letsencrypt.conf"
cp "$here/nginx/snippets/security.conf" "/etc/nginx/snippets/security.conf"
cp "$here/nginx/snippets/ssl.conf" "/etc/nginx/snippets/ssl.conf"
cp "$here/nginx/snippets/options-proxypass.conf" "/etc/nginx/snippets/options-proxypass.conf"

log "Disable default site"
rm -f /etc/nginx/sites-enabled/default

log "Enable our http config"
ln -f -s ../sites-available/proxy /etc/nginx/sites-enabled/proxy

log "Set database and nomad server IP in web server config"
sed -i "s/CSI_NOMAD_SERVER_IP/$CSI_NOMAD_SERVER_IP/" /etc/nginx/sites-available/proxy_ssl
sed -i "s/CSI_HTTP_API_INSTANCE_IP/$CSI_HTTP_API_INSTANCE_IP/" /etc/nginx/sites-available/proxy_ssl

log "Reload config"
systemctl reload nginx.service

log "Setup letsencrypt certificates"
rclone copy --links foo/hidden_value/tf-proxy/letsencrypt /etc/letsencrypt
mkdir -p /etc/letsencrypt/renewal-hooks/deploy
mkdir -p /etc/letsencrypt/conf.d

cp "$here/letsencrypt/deploy-hook" /etc/letsencrypt/renewal-hooks/deploy/deploy-hook
if [[ "$OS_PROJECT_ID" == "hidden_value" ]]; then
    log "test project detected, using test domain names"
    cp "$here/letsencrypt/bar.ini" /etc/letsencrypt/conf.d/cosims.ini
elif [[ "$OS_PROJECT_ID" == "hidden_value" ]]; then
    log "prod project detected, using prod domain names"
    cp "$here/letsencrypt/cosims.ini" /etc/letsencrypt/conf.d/cosims.ini
else
    log "ERROR unknown project if $OS_PROJECT_ID"
    exit 1
fi
chmod 755 /etc/letsencrypt/renewal-hooks/deploy/deploy-hook

log "Install certbot for certificate generation/renewal"
apt-get install -y certbot
systemctl enable certbot.timer
systemctl start certbot.timer

log "Check if certificate needs generation"
if ! [ -e /etc/letsencrypt/live/cosims/cert.pem ]; then
    certbot -n --agree-tos --config /etc/letsencrypt/conf.d/cosims.ini certonly
    /etc/letsencrypt/renewal-hooks/deploy/deploy-hook
else
    certbot renew
fi

log "Enable our https config"
ln -f -s ../sites-available/proxy_ssl /etc/nginx/sites-enabled/proxy_ssl

log "Reload config"
systemctl reload nginx.service

# A key to emit in the log so that is can be searched by any one that want to
# automatically check if the script finished with success.
init_success_key="init_instance_finished_with_success"
log "$init_success_key"

log "done"
