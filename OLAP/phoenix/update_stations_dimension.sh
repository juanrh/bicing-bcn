#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_SCRIPT_PATH=${SCRIPT_DIR}/"$(basename ${0%.*})"
UPDATE_SCRIPT="${BASE_SCRIPT_PATH}.py"
UPDATE_SQL_SCRIPT="${BASE_SCRIPT_PATH}.sql"
BICING_HOME='/usr/lib/bicingbcn/'
PHOENIX_HOME=${BICING_HOME}/"lib/phoenix-*"
TABLE_NAME='BICING_DIM_STATION'

echo "Starting update of ${TABLE_NAME}"

echo "Generating sql script"
pushd ${SCRIPT_DIR}
# FIXME python2.7 ${UPDATE_SCRIPT}
popd
echo "Done generating sql script"

echo "Running sql script"
pushd ${PHOENIX_HOME}
./bin/sqlline.py localhost < ${UPDATE_SQL_SCRIPT}
popd
echo "Done running sql script"

echo "Done updating of ${TABLE_NAME}"