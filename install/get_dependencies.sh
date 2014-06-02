#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source ${SCRIPT_DIR}/config.sh

rm -rf ${DEPS_TRG_DIR}
mkdir -p ${DEPS_TRG_DIR}
pushd ${DEPS_TRG_DIR}
wget ${DEPS_ROOT}/${DEPS_LIST}
cat ${DEPS_LIST} | while read dep_url
do
    wget ${DEPS_ROOT}/${dep_url}
done 

popd