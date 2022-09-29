set -e

apt-get update --fix-missing

apt-get install -y build-essential tar file apt-utils pkg-config wget curl
apt-get install -y docker.io

apt-get install -y python3 python3-pip python3-venv
rm -f $(which python); ln -s $(which python3) /usr/bin/python

cd
mkdir ~/dev
cd ~/dev
git clone https://gitana-ext.magellium.com/cosims/cosims.git
cd cosims
git checkout 235-cnes-cluster-reprocessing-chain

cd
curl --silent --remote-name  https://downloads.rclone.org/v1.50.2/rclone-v1.50.2-linux-amd64.zip; \
    unzip rclone-v1.50.2-linux-amd64.zip; \
    mv rclone-v1.50.2-linux-amd64/rclone /usr/local/bin/; \
    rm -Rf rclone-v1.50.2-linux-amd64*
mkdir -p ~/.config/rclone
mv ~/rclone.conf ~/.config/rclone/

cd
rclone copy foo:path_to_tar .
tar -xf docker_install_bundle_10.tar
rm -f docker_install_bundle_10.tar
mv docker_install_bundle_10 ~/docker_install_bundle

cd
rclone copy bar:work/remi_share/si_software_images/si_software_base_latest.tar.gz .
docker load < si_software_base_latest.tar.gz
rm -f si_software_base_latest.tar.gz

cd ~/dev/cosims/components/si_software/docker
./run_vm
