#!/bin/sh 
#
#  ae_htload.sh
###########################################################################
#
#  Purpose:
# 	This script QCs and loads raw GXD High Throughput ArrayExpress
#       data
#
#  Usage=ae_htload.sh
#
#  Env Vars:
#
#      See the configuration file
#
#  Inputs:
#
#      - Common configuration file -
#               /usr/local/mgi/live/mgiconfig/master.config.sh
#      - load configuration file - ae_htload.config
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
# sc	12/27/2023 - WTS2-545
#

cd `dirname $0`

CONFIG_LOAD=ae_htload.config

USAGE="Usage: ae_htload.sh"

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
if [ -f ../${CONFIG_LOAD} ]
then
    . ../${CONFIG_LOAD}
else
    echo "Missing configuration file: ${CONFIG_LOAD}"
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

#####################################
#
# Main
#
#####################################

#
# createArchive including OUTPUTDIR, startLog, getConfigEnv
# sets "JOBKEY"
preload #${OUTPUTDIR}

#
#  run the load
#
echo 'Running ae_htload.py'  | tee -a ${LOG_DIAG}
${PYTHON} ${GXDHTLOAD}/bin/ae_htload.py >> ${LOG_DIAG}
STAT=$?
checkStatus ${STAT} "${GXDHTLOAD}/bin/ae_htload.py"

#
# run postload cleanup and email logs
#
shutDown

