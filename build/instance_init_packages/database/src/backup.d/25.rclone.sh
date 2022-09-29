#!/bin/sh

when = everyday at 04:00

rclone sync /backup/tf-tutu foo/hidden_value/tf-tutu || fatal "Unable to sync borg repo using rclone"
