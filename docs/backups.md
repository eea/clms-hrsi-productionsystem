# Backups

This document describes the backup and restore process of the platform.

## System description and setup

### Perimeter

Only the contents of the database are saved

### Frequency and retention

A new backup is created every day at 4h00 UTC.
Backups are kept:

* 7 days
* 1 backup per week for 5 weeks
* 1 by month for a year

### Destinations

The backups are stored at three different locations:

* On the *tf-tutu* VM in the directory `/backup/tf-tutu`
* This directory is synced with the *hidden_value* bucket in `/hidden_value/tf-tutu`
* Each backup is copied to a remote location: on the VM `monitoring-eo.magellium.fr` hosted in Magellium server room.

### Monitoring

If an error occures during the backup process, a mail is sent to `cosims-monitor@magellium.fr` (mailing list).

### Details

The backup process is based on 5 main components:

* pg_dumpall is used to dump the database
* [borg](https://borgbackup.readthedocs.io), a deduplicating backup program stores efficiently the backups
* backupninja, a lightweight, extensible meta-backup system used to configure and schedule backup executions
* nullmailer, a simple relay-only mail transport agent to allow backupninja to notify a backup problem
* [rclone](https://rclone.org), used to copy borg files to the bucket

During the *tf-tutu* VM initialization, the init script will copy the initial content of `foo/hidden_value/tf-tutu` to the local directory `/backup/tf-tutu`.
Hence, a rebuild from scratch of the database volume will preserve those backups.

Communication between the *tf-tutu* VM and *monitoring-eo* VM is managed by using SSH and a SSH key.
The keys are not deployed using the initialization script to the *monitoring-eo* VM and thus must be added to the authorized_keys by hand. This is a one time step since the same key will be deployed on the *tf-tutu* VM in case of rebuild.

Here is the current key and content of the authorized_keys on the monitoring-eo VM:

```shell
command="export BORG_BASE_DIR=/home/cosims/borg; borg serve --restrict-to-path /home/cosims/borg",restrict ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDXkFofGfwkeUMT8Vyfp1D3SE80tSecQ/qCbqS0dYnnJT8Q/ozFIoCPzhlkgmSNoydPgu/s84mt4wo++gHJ5UNaaYvaLxZvMLMfyYrURQUHsltnhafUq1UgawXJIVR35/cZoN6rzJ0D2R1vHuBTayoTOd6TJrn/BpbY14YKJ889pN643flkJ8LikDIxoloUTYA23wwg2/k7RYnBOGD7HRdP8tMCB+7uj7elOfKW/N3EWg/EvSHRcsqSrTFROIxez5D2kkSe3STat6rFEzUxuwL+erC6fcFjqBNamPIwHDJGy5a4tyyvv7MEPAnPQXcl7DrN4rni/R3UWkO5TIN6373/
```

Nullmailer is configured to forward all local mails to cosims-monitor@magellium.fr  

The VM initialization script installs all the requirements and copies the backups in config files into the directory `/etc/backup.d`.
This directory contains three configuration files:

* `20.pgdump.sh` dumps the database with `pg_dumpall` and directly pipes its output (no intermediate file) into a new `borg` archive into the `borg` repository `/backup/tf-tutu`. Eventually, it prunes the old `borg` archive in that repository
* `25.rclone.sh` syncs the local borg repository in `/backup/tf-tutu` to the buck `/hidden_value/tf-tutu`
* `50.pgdump_export.sh` exports the content of the last archive in the local borg repository, to the remote repository `ssh://cosims@monitoring-eo.magellium.fr:17924/~/tf-tutu`. Eventually, it prunes the remote repository.

## Restoration

To restore a backup one needs to:

* Extract the database dump to restore from the `borg` repository
* Load it into the database.

As the backup is a complete dump of the database, please restore it to an empty database.

Borg repositories are available:

* On the *tf-tutu* VM in the directory `/backup/tf-tutu`
* In the hidden_value bucket in `/hidden_value/tf-tutu`
* On the *monitoring-eo.magellium.fr* VM hosted in Magellium server room.

To use the repository in the hidden_value bucket you **MUST** copy it to a local file system.
See the [borg documentation](https://borgbackup.readthedocs.io) for how to use borg.
The main commands you will need are:

* [borg list](https://borgbackup.readthedocs.io/en/stable/usage/list.html) to list available backup and choose an archive

   ```shell
   root@tf-tutu:~# borg list /backup/tf-tutu
   pgdump_2020-05-31T01:00:03           Sun, 2020-05-31 01:00:03 [64b7f3c414d8ee3330a896076af1ffcff8f742b06b1423b3739ae49b303b8c7b]
   pgdump_2020-06-07T04:00:06           Sun, 2020-06-07 04:00:06 [a7db6c77e439c0e8d497a3b28164327e129b4c651d784f5cdd9bd30300cc9ad3]
   pgdump_2020-06-14T04:00:02           Sun, 2020-06-14 04:00:02 [f8c4eb63d3e8674fe4b5f2e56683c950d97e59268048111043506176edbaf2b5]
   pgdump_2020-06-20T04:00:04           Sat, 2020-06-20 04:00:04 [6481a57569cdff2d2e9127b05c1e19629c95ac59c64bc45dc34fac86cfd4334d]
   pgdump_2020-06-21T04:00:02           Sun, 2020-06-21 04:00:02 [44cc97f4792af0ef09ae7a0b08ac13b5b512933828345486dfaa692517a6fb53]
   pgdump_2020-06-22T04:00:02           Mon, 2020-06-22 04:00:02 [28bac3b195e630b582f49063d6b82b7944c133ee77ebc7448e8666fa736f9f1c]
   pgdump_2020-06-23T04:00:02           Tue, 2020-06-23 04:00:03 [ced2824cdb114ae829bb2d2769c54e4e64dd7b9799f8e0a388187e41e991bdfc]
   pgdump_2020-06-24T04:00:04           Wed, 2020-06-24 04:00:05 [cff39a45e478c6c83a07137e36ef409219b332f8ca96cb524241247792c0d9b6]
   pgdump_2020-06-25T04:00:02           Thu, 2020-06-25 04:00:02 [60cdb8dc7fb282417ab609d37465437daa6b7ee9684a3178198da0b354e40d0c]
   pgdump_2020-06-26T04:00:02           Fri, 2020-06-26 04:00:02 [b92c0fe0a1189d7e0e14318d075e4590d3505979ff1bd174c29f8dab4ea0f3b3]
  ```

* You can also see the content of an archive with the list command
  
  ```shell
  root@tf-tutu:~# borg list /backup/tf-tutu::pgdump_2020-06-26T04:00:02
  -rw-rw---- root   root   89954536 Fri, 2020-06-26 04:00:02 database-all.sql
  ```
  
* [borg extract](https://borgbackup.readthedocs.io/en/stable/usage/extract.html) to extract the content of an archive
  
  ```shell
  root@tf-tutu:~# borg extract /backup/tf-tutu::pgdump_2020-06-26T04:00:02 database-all.sql
  ```

* Load the file `database-all.sql` into the database:

  ```shell
  root@tf-tutu:~# sudo -u postgres psql -f database-all.sql
  ```

  This command can be conbined with the previous one to avoid having an intermediate file:

  ```shell
  root@tf-tutu:~# borg extract --stdout /backup/tf-tutu::pgdump_2020-06-26T04:00:02 database-all.sql | sudo -u postgres psql
  ```
