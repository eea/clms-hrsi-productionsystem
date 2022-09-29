# DISCLAMER: this shell script is executed by backupninja
# thought /usr/share/backupninja/sh by sourcing it

when = everyday at 04:00

directory=/backup/tf-tutu
archive=pgdump_{now:%Y-%m-%dT%H:%M:%S}
prune_options="--keep-within 7d --keep-weekly 4 --keep-monthly 12 --prefix pgdump_"
compression=zstd
encryption=none
passphrase=""


export BORG_RELOCATED_REPO_ACCESS_IS_OK=yes
export BORG_UNKNOWN_UNENCRYPTED_REPO_ACCESS_IS_OK=yes
export BORG_PASSPHRASE="$passphrase"

# check borg repo initialisation
initstr="borg init --encryption=$encryption $directory"
debug "$initstr"
output="`su -c "$initstr" 2>&1`"
if [ $? = 2 ]; then
   debug $output
   info "Repository was already initialized"
else
   warning $output
   warning "Repository has been initialized"
fi


# dump database directly into borg
borg_create="borg create --stats --compression $compression  --stdin-name database-all.sql $directory::$archive -"
execstr="set -o pipefail ; cd /tmp; sudo -u postgres /usr/bin/pg_dumpall | $borg_create"
debug "$execstr"
output=`/bin/bash -c "$execstr" 2>&1`
code=$?
if [ "$code" == "0" ]; then
    debug $output
    info "Successfully finished dump of pgsql cluster"
else
    error $output
    error "Failed to dump pgsql cluster"
fi


# remove old archives
prunestr="borg prune $prune_options $directory"
debug "$prunestr"
output="`su -c "$prunestr" 2>&1`"
if [ $? = 0 ]; then
   debug $output
   info "Removing old backups succeeded."
else
   warning $output
   warning "Failed removing old backups."
fi
