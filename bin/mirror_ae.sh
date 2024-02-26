#!/bin/sh
#
#  mirror_ae.sh
###########################################################################
#
#  Purpose:
#       This script mirrors the experiment input files for the GEO gxdhtload
#       It is a CONVENIENCE script so you may run the download separately from
#       ae_htload.sh
#
#  Usage=mirror_ae.sh
#
# History:
#
# sc    12/29/2023 - WTS2-545

cd `dirname $0`

CONFIG_LOAD=../ae_htload.config

#
# verify & source the configuration file
#

if [ ! -r ${CONFIG_LOAD} ]
then
    echo "Cannot read configuration file: ${CONFIG_LOAD}"
    exit 1
fi

. ${CONFIG_LOAD}

LOG=${MIRROR_LOG_FILE}
rm -rf ${LOG}
touch ${LOG}

echo 'Running mirror_ae.py'  | tee -a ${LOG}
date | tee -a ${LOG}
${PYTHON} mirror_ae.py >> ${LOG}
STAT=$?
echo "STAT: ${STAT}"

date | tee -a ${LOG} 
