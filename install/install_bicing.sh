#!/bin/bash

# TODO: installer for Apache Phoenix client, and server
#  [cloudera@localhost phoenix3]$ sudo cp -r phoenix-3.0.0-incubating /usr/lib/bicingbcn/
# TODO: Spark
# TODO: Storm

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${SCRIPT_DIR}/config.sh

echo "Installing bicingbcn"
pushd ${SCRIPT_DIR}

echo "Downloading dependencies"
sudo ./get_dependencies.sh
echo "Done downloading dependencies"

sudo rm -rf ${BICING_BASEDIR}
sudo mkdir -p ${BICING_BASEDIR}
sudo mkdir -p ${BICING_BASEDIR}/lib
sudo mkdir -p ${BICING_BASEDIR}/bin
sudo chown -R ${USER}:${USER} ${BICING_BASEDIR} 

ls -1 installers | while read script
do
    ./installers/${script}
done 

cp -rv bin ${BICING_BASEDIR}/
cp -v config.sh ${BICING_BASEDIR}/bin
echo "Done installing bicingbcn"
popd
