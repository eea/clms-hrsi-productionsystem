#!/bin/bash

# Be sure we fail on error and output debugging information
set -e
trap 'echo "$0: error on line $LINENO"' ERR

echo "wait in case apt is already running..."
sleep 15

# Disable some warning and questions from apt-get
export DEBIAN_FRONTEND=noninteractive

echo "install some dependencies"
sudo apt-get update
sudo apt-get install --yes \
  unzip \
  curl \
  docker.io \
  jq

echo "install openstack"
sudo apt install --yes python3-openstackclient

echo "install Node.js and npm"
curl -sL https://deb.nodesource.com/setup_12.x | sudo -E bash -
sudo apt install --yes nodejs

echo "install rclone"
curl --silent --remote-name  https://downloads.rclone.org/v1.50.2/rclone-v1.50.2-linux-amd64.zip
unzip rclone-v1.50.2-linux-amd64.zip
sudo chown root:root rclone-v1.50.2-linux-amd64/rclone
sudo mv rclone-v1.50.2-linux-amd64/rclone /usr/local/bin/
sudo rm rclone-v1.50.2-linux-amd64.zip
sudo rm -rf rclone-v1.50.2-linux-amd64
sudo chown root:root /home/eouser/rclone.conf
sudo mv /home/eouser/rclone.conf /usr/local/bin/

echo "prepare the default working directory for CoSIMS"
sudo mkdir -p /opt/csi/work
