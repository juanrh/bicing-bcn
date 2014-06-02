#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

BICING_BASEDIR="/usr/lib/bicingbcn"

DEPS_ROOT="https://s3.amazonaws.com/juanrh.bicingbcn/pub/sw_dependencies"
DEPS_LIST="list.txt"
DEPS_TRG_DIR=${SCRIPT_DIR}/deps

function report_start {
    COMPONENT=$1
    EXIT_CODE=$2

    if [ ${EXIT_CODE} -ne 0 ]
    then 
        echo "Error starting component ${COMPONENT}, exit code ${EXIT_CODE}"
    else 
        echo "${COMPONENT} started correctly"
    fi
}
