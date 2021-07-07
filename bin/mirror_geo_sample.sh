#!/bin/sh
#
#  mirror_geo_sample.sh
###########################################################################
cd `dirname $0`

COMMON_CONFIG=geo_htload.config

#
# Make sure the common configuration file exists and source it.
#
if [ -f ../${COMMON_CONFIG} ]
then
    . ../${COMMON_CONFIG}
else
    echo "Missing configuration file: ${COMMON_CONFIG}"
    exit 1
fi

#
# Initialize the log file.
#
LOG="${GEO_LOG_FILE}.sample"
rm -rf ${LOG}
touch ${LOG}

cd ${GEO_DOWNLOADS}

ALL_FILES=`ls ${GEO_DOWNLOADS}/geo.xml.*`
#ALL_FILES="${GEO_DOWNLOADS}/geo.xml.1"
export ALL_FILES

echo 'Running mirror_geo_sample.py'  | tee -a ${LOG}
${PYTHON} ${GXDHTLOAD}/bin/mirror_geo_sample.py >> ${LOG}
STAT=$?
echo "STAT: ${STAT}"
