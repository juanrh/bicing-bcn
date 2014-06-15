#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${SCRIPT_DIR}/config.sh

echo "Starting bicingbcn"
################
# Apache Kafka
################
echo "Starting Apache Kafka"
pushd ${SCRIPT_DIR}
pushd ../lib/kafka_*
    # Zookeeper is already started in Cloudera and Hortonworks
# bin/zookeeper-server-start.sh config/zookeeper.properties &> /dev/null &
bin/kafka-server-start.sh config/server.properties &> /dev/null &
RET_CODE=$?
report_start "Apache Kafka" ${RET_CODE}
popd
popd
################
# Redis
################
# FIXME depends on Centos installation, pretty standard anyway
# This includes suitable stdout messages
sudo service redis start
###################
echo "Done starting bicingbcn"