#!/bin/sh
#
#  gxdhtload.sh
###########################################################################
#
#  Purpose:
# 	This script QCs and loads raw GXD High Throughput ArrayExpress
#       data
#
#  Usage=gxdhtload.sh
#
#  Env Vars:
#
#      See the configuration file
#
#  Inputs:
#
#      - Common configuration file -
#               /usr/local/mgi/live/mgiconfig/master.config.sh
#      - load configuration file - gxdhtload.config
#      - input file - see python script header
#
#  Outputs:
#
#      	- An archive file
#      	- Log files defined by the environment variables ${LOG_PROC},
#        ${LOG_DIAG}, ${LOG_CUR} and ${LOG_VAL}
#	- QC report written to ${RPTDIR}
#	- bcp files
#      	- Records written to the database tables
#      	- Exceptions written to standard error
#      	- Configuration and initialization errors are written to a log file
#        for the shell script
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  Fatal error occurred
#      2:  Non-fatal error occurred
#
#  Assumes:  Nothing
#
# History:
#
# sc	08/25/2016 - TR12370
#

cd `dirname $0`

COMMON_CONFIG=gxdhtload.config

USAGE="Usage: gxdhtload.sh"

#
# BCP delimiters
#
COLDELIM="\t"
LINEDELIM="\n"

#
#  Verify the argument(s) to the shell script.
#
if [ $# -ne 0 ]
then
    echo ${USAGE} | tee -a ${LOG}
    exit 1
fi

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
LOG=${LOG_FILE}
rm -rf ${LOG}
touch ${LOG}

#
#  Source the DLA library functions.
#

if [ "${DLAJOBSTREAMFUNC}" != "" ]
then
    if [ -r ${DLAJOBSTREAMFUNC} ]
    then
        . ${DLAJOBSTREAMFUNC}
    else
        echo "Cannot source DLA functions script: ${DLAJOBSTREAMFUNC}" | tee -a ${LOG}
        exit 1
    fi
else
    echo "Environment variable DLAJOBSTREAMFUNC has not been defined." | tee -a ${LOG}
    exit 1
fi

#
# verify input file exists and is readable
#

if [ ! -r ${INPUT_FILE_DEFAULT} ]
then
    # set STAT for endJobStream.py
    STAT=1
    checkStatus ${STAT} "Cannot read from input file: ${INPUT_FILE_DEFAULT}"
fi

#####################################
#
# Main
#
#####################################

#
# createArchive including OUTPUTDIR, startLog, getConfigEnv
# sets "JOBKEY"
preload ${OUTPUTDIR}

#
#  run the load
#
echo 'Running gxdhtload.py'  | tee -a ${LOG_DIAG}
${GXDHTLOAD}/bin/gxdhtload.py #>> ${LOG_DIAG}
STAT=$?
checkStatus ${STAT} "${GXDHTLOAD}/bin/gxdhtload.py"

#
# Do BCP
#
TABLE=GXD_HTExperiment

if [ -s "${OUTPUTDIR}/${TABLE}.bcp" ]
then

    echo "" >> ${LOG_DIAG}
    date >> ${LOG_DIAG}
    echo "BCP data into ${TABLE}"  >> ${LOG_DIAG}

    # BCP new data
    ${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ${TABLE} ${OUTPUTDIR} ${TABLE}.bcp ${COLDELIM} ${LINEDELIM} >> ${LOG_DIAG}
fi

TABLE=ACC_Accession

if [ -s "${OUTPUTDIR}/${TABLE}.bcp" ]
then

    echo "" >> ${LOG_DIAG}
    date >> ${LOG_DIAG}
    echo "BCP data into ${TABLE}"  >> ${LOG_DIAG}

    # BCP new data
    ${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ${TABLE} ${OUTPUTDIR} ${TABLE}.bcp ${COLDELIM} ${LINEDELIM} >> ${LOG_DIAG}
fi

TABLE=GXD_HTExperimentVariable

if [ -s "${OUTPUTDIR}/${TABLE}.bcp" ]
then

    echo "" >> ${LOG_DIAG}
    date >> ${LOG_DIAG}
    echo "BCP data into ${TABLE}"  >> ${LOG_DIAG}

    # BCP new data
    ${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ${TABLE} ${OUTPUTDIR} ${TABLE}.bcp ${COLDELIM} ${LINEDELIM} >> ${LOG_DIAG}
fi

TABLE=MGI_Property

if [ -s "${OUTPUTDIR}/${TABLE}.bcp" ]
then

    echo "" >> ${LOG_DIAG}
    date >> ${LOG_DIAG}
    echo "BCP data into ${TABLE}"  >> ${LOG_DIAG}

    # BCP new data
    ${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ${TABLE} ${OUTPUTDIR} ${TABLE}.bcp ${COLDELIM} ${LINEDELIM} >> ${LOG_DIAG}
fi

#
# run postload cleanup and email logs
#
shutDown

