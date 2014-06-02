#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${SCRIPT_DIR}/config.sh

echo "Installing bicingbcn"
pushd ${SCRIPT_DIR}
sudo rm -rf ${BICING_BASEDIR}
sudo mkdir -p ${BICING_BASEDIR}
sudo chown -R ${USER}:${USER} ${BICING_BASEDIR} 

ls -1 installers | while read script
do
    ./installers/${script}
done 

echo "Done installing bicingbcn"
popd