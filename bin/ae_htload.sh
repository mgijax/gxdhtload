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

rm -rf ${MIRROR_LOG_CUR}
touch  ${MIRROR_LOG_CUR}

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
# rm all files/dirs from OUTPUTDIR
#

cleanDir ${OUTPUTDIR}

# NOTE: keep this commented out until production release
#
# There should be a "lastrun" file in the input directory that was created
# the last time the load was run for this input file. If this file exists
# and is more recent than the input file, the load does not need to be run.
#
LASTRUN_FILE=${INPUTDIR}/lastrun
if [ -f ${LASTRUN_FILE} ]
then
    if test ${LASTRUN_FILE} -nt ${INPUT_FILE_DEFAULT}
    then

        echo "Input file has not been updated - skipping load" | tee -a ${LOG_PROC}
        # set STAT for shutdown
        STAT=0
        echo 'shutting down'
        shutDown
        exit 0
    fi
fi


#
#  run the file download
#

LOG=${MIRROR_LOG_FILE}
rm -rf ${LOG}
touch ${LOG}

echo 'Running mirror_ae.py'  | tee -a ${LOG}
date | tee -a ${LOG}
${PYTHON} -W "igmore" mirror_ae.py >> ${LOG}
STAT=$?
echo "STAT: ${STAT}"

if [ ${STAT} -eq 1 ]
then
    checkStatus ${STAT} "An error occurred while downloading the experiment/sample files - See ${MIRROR_LOG_CUR} mirror_ae.sh"

fi

for i in `echo ${MAIL_LOG_CUR} | sed 's/,/ /g'`
do
    mailx -s "${MAIL_LOADNAME} - AE Experiment/Sample Download Curator Log" ${i} < ${MIRROR_LOG_CUR}
done

echo "" >> ${LOG_DIAG}
date >> ${LOG_DIAG}
echo "Running QC checks"  | tee -a ${LOG_DIAG}
${GXDHTLOAD}/bin/exptQC.sh ${INPUT_FILE_DEFAULT} live
STAT=$?

if [ ${STAT} -eq 1 ]
then
    checkStatus ${STAT} "An error occurred while generating the QC reports - See ${QC_LOGFILE} exptQC.sh"

    # run postload cleanup and email logs
    shutDown
fi

if [ ${STAT} -eq 2 ]
then
    checkStatus ${STAT} "QC errors detected. See ${QC_RPT}. exptQC.sh"

    # run postload cleanup and email logs
    shutDown

fi

#
#  run the load
#
echo 'Running ae_htload.py'  | tee -a ${LOG_DIAG}
${PYTHON} ${GXDHTLOAD}/bin/ae_htload.py >> ${LOG_DIAG}
STAT=$?
checkStatus ${STAT} "${GXDHTLOAD}/bin/ae_htload.py"

#
# Touch the "lastrun" file to note when the load was run.
#
if [ ${STAT} = 0 ]
then
    touch ${LASTRUN_FILE}
fi

#
# run postload cleanup and email logs
#
shutDown

