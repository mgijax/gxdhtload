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
LOG="${GEO_MIRROR_LOG_FILE}.sample"
rm -rf ${LOG}
touch ${LOG}

cd ${GEO_DOWNLOADS}

# for testing one expt file at a time
#ALL_FILES="${GEO_DOWNLOADS}/geo.xml.1"

ALL_FILES=`ls ${GEO_DOWNLOADS}/geo.xml.*`

export ALL_FILES

echo 'Running mirror_geo_sample.py'  | tee -a ${LOG}
date | tee -a ${LOG}
${PYTHON} ${GXDHTLOAD}/bin/mirror_geo_sample.py >> ${LOG}
STAT=$?
echo "STAT: ${STAT}"

for i in `echo ${MAIL_LOG_CUR} | sed 's/,/ /g'`
do
    mailx -s "${MAIL_LOADNAME} - Sample Download Curator Log" ${i} < ${MIRROR_LOG_CUR}
done

date | tee -a ${LOG} 
