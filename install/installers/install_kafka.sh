#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${SCRIPT_DIR}/../config.sh

echo "Installing Apache Kafka"
pushd ${BICING_BASEDIR}/lib
cp ${DEPS_TRG_DIR}/kafka_*.tgz .
tar xvzf kafka_*.tgz
rm -f kafka_*.tgz
popd

echo "Done installing Apache Kafka"