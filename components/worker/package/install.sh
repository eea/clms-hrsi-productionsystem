#!/usr/bin/env bash

# Be sure we fail on error and output debugging information
set -e
trap 'echo "$0: error on line $LINENO"' ERR

here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

#-------------------------------------------------------------------------

apt-get install python3-setuptools python3-pip
pip3 install --upgrade pip
pip3 install wheel
pip3 install --upgrade cython

pip3 install -r "$here/components/common/python/requirements.txt"
pip3 install --ignore-installed -r "$here/components/worker/python/requirements.txt"
