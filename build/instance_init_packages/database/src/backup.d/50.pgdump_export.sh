# DISCLAMER: this shell script is executed by backupninja
# thought /usr/share/backupninja/sh by sourcing it

when = everyday at 04:00

src_directory=/backup/tf-tutu
directory=ssh://cosims@monitoring-eo.magellium.fr:17924/~/tf-tutu
identity_file=/root/.ssh/csi_database_id_rsa
prefix=pgdump_
prune_options="--keep-within 7d --keep-weekly 4 --keep-monthly 12 --prefix $prefix"
compression=zstd
encryption=none
passphrase=""


export BORG_RELOCATED_REPO_ACCESS_IS_OK=yes
export BORG_UNKNOWN_UNENCRYPTED_REPO_ACCESS_IS_OK=yes
export BORG_PASSPHRASE="$passphrase"
if [ -n "$identity_file" ]; then
    export BORG_RSH="ssh -i $identity_file"
    if ! [ -f "$identity_file" ]; then
        fatal "SSH identity file $identity_file not found"
    fi
fi

archive=$(borg list --last 1 --prefix $prefix --short $src_directory)

# check borg repo initialisation
initstr="borg init --encryption=$encryption $directory"
debug "$initstr"
output="`su -c "$initstr" 2>&1`"
code=$?
if [ "$code" = "2" ]; then
   debug $output
   info "Repository was already initialized"
elif [ "$code" = "0" ]; then
   warning $output
   warning "Repository has been initialized"
else
   error $output
   error "Fail to initialize repository"
fi


# copy dump from last local backup to remote backup
borg_create="borg create --stats --compression $compression  --stdin-name database-all.sql $directory::$archive -"
execstr="set -o pipefail ; borg extract  --stdout /backup/tf-tutu::$archive database-all.sql | $borg_create"
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
