### backups setup
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

# backupninja bakup configs
# pgsql dump
assert_file_existence "$here/backup.d/20.pgdump.sh"
# export to s3 bucket using rclone
assert_file_existence "$here/backup.d/25.rclone.sh"
# copy of last dump to external backup
assert_file_existence "$here/backup.d/50.pgdump_export.sh"

# The SSH key used to connect to Magellium backup host
assert_file_existence /root/.ssh/csi_database_id_rsa

if [ $at_least_one_assert_failed = true ]
then
  log "[asserts error] at least one assert failed (see previous assert log messages)"
  exit 1
else
  log "[asserts ok] all asserts are ok"
fi


log "install backup tools"
apt-get update
DEBIAN_FRONTEND=noninteractive apt install --no-install-recommends --yes backupninja borgbackup nullmailer

log "configure backup email notification"
# all local mails will be forwarded to this address
echo "cosims-monitor@magellium.fr" > /etc/nullmailer/adminaddr
# all mails are relayed thought this server
echo "smtp.magellium.fr" > /etc/nullmailer/remotes
# all mails enveloppe from are set to this mail address
echo "cosims-monitor@magellium.fr" > /etc/nullmailer/allmailfrom

log "configure backupninja email reporting"
sed -i 's/^reportsuccess = yes/reportsuccess = no/' /etc/backupninja.conf
sed -i 's/^reportinfo = no/reportinfo = yes/' /etc/backupninja.conf

mkdir -p /backup
if ! [ -d /backup/tf-tutu ]; then
    log "Copy backup from bucket to local filesystem"
    rclone copy foo/hidden_value/tf-tutu /backup/tf-tutu
fi

# if the directory is empty it must be remove to prevent error upon borg
# initialisation. If it is not empty, it's all good
rmdir /backup/tf-tutu 2>/dev/null || true

log "configure backups"
rm /etc/backup.d/* 2>/dev/null || true
cp "$here/backup.d/20.pgdump.sh" "/etc/backup.d/20.pgdump.sh"
cp "$here/backup.d/25.rclone.sh" "/etc/backup.d/25.rclone.sh"
cp "$here/backup.d/50.pgdump_export.sh" "/etc/backup.d/50.pgdump_export.sh"
chmod 600 /etc/backup.d/*

log "Retreive backup server ssh public keys to known_hosts"
[ -f /root/.ssh/known_hosts ] && sed -i '/monitoring-eo.magellium.fr/d' /root/.ssh/known_hosts
ssh-keyscan -p 17924 monitoring-eo.magellium.fr >> /root/.ssh/known_hosts

log "done"
