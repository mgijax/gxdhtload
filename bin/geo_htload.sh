#!/bin/sh 
#
#  geo_htload.sh
###########################################################################
#
#  Purpose:
# 	This script QCs and loads raw GXD High Throughput GEO
#       data
#
#  Usage=geo_htload.sh
#
#  Env Vars:
#
#      See the configuration file
#
#  Inputs:
#
#      - Common configuration file -
#               /usr/local/mgi/live/mgiconfig/master.config.sh
#      - load configuration file - geo_htload.config
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
# sc	06/17/2021 - WTS2-431
#

cd `dirname $0`

COMMON_CONFIG=geo_htload.config

USAGE="Usage: geo_htload.sh"

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
preload #${OUTPUTDIR}

# get the listing of experiment files
# for debug, process just one file
#EXP_FILES=`ls ${GEO_DOWNLOADS}/geo.xml.10`

EXP_FILES=`ls ${GEO_DOWNLOADS}/geo.xml.*`
export EXP_FILES

echo "EXP_FILES: ${EXP_FILES}"
#
#  run the load
#
echo 'Running geo_htload.py'  | tee -a ${LOG_DIAG}
${PYTHON} ${GXDHTLOAD}/bin/geo_htload.py >> ${LOG_DIAG}
STAT=$?
checkStatus ${STAT} "${GXDHTLOAD}/bin/geo_htload.py"

#
# Truncate the Raw Sample table
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Truncate GXD_HTRawSample" | tee -a ${LOG}
${MGD_DBSCHEMADIR}/table/GXD_HTRawSample_truncate.object
echo "" >> ${LOG}

#
# run BCP
#

date >> ${LOG}
echo "Load GXD_HTExperiment" | tee -a ${LOG}
${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} GXD_HTExperiment ${OUTPUTDIR} GXD_HTExperiment.bcp "\t" "\n" mgd
date >> ${LOG}

echo "" >> ${LOG}
date >> ${LOG}
echo "Load ACC_Accession" | tee -a ${LOG}
${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ACC_Accession ${OUTPUTDIR} ACC_Accession.bcp "\t" "\n" mgd
date >> ${LOG}

echo "" >> ${LOG}
date >> ${LOG}
echo "Load GXD_HTExperimentVariable" | tee -a ${LOG}
${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} GXD_HTExperimentVariable ${OUTPUTDIR} GXD_HTExperimentVariable.bcp "\t" "\n" mgd
date >> ${LOG}

echo "" >> ${LOG}
date >> ${LOG}
echo "Load MGI_Property" | tee -a ${LOG}
${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} MGI_Property ${OUTPUTDIR} MGI_Property.bcp "\t" "\n" mgd
date >> ${LOG}

echo "" >> ${LOG}
date >> ${LOG}
echo "Load GXD_HTRawSample" | tee -a ${LOG}
${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} GXD_HTRawSample ${OUTPUTDIR} GXD_HTRawSample.bcp "\t" "\n" mgd
date >> ${LOG}

echo "" >> ${LOG}
date >> ${LOG}
echo "Load MGI_KeyValue" | tee -a ${LOG}
${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} MGI_KeyValue ${OUTPUTDIR} MGI_KeyValue.bcp "\t" "\n" mgd
date >> ${LOG}

echo "" >> ${LOG}
date >> ${LOG}
echo "Load MGI_Note" | tee -a ${LOG}
${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} MGI_Note ${OUTPUTDIR} MGI_Note.bcp "\t" "\n" mgd
date >> ${LOG}

#
# Update autosequence
# 

echo "Updating auto-sequence" >>  ${LOG}
cat - <<EOSQL | psql -h${MGD_DBSERVER} -d${MGD_DBNAME} -U mgd_dbo -e >> ${LOG_DIAG} 2>&1

select setval('gxd_htexperiment_seq', (select max(_Experiment_key) from GXD_HTExperiment)) 
;

select setval('gxd_htexperimentvariable_seq', (select max(_ExperimentVariable_key) from GXD_HTExperimentVariable))
;

select setval('gxd_htrawsample_seq', (select max(_RawSample_key) from GXD_HTRawSample))
;

select setval('mgi_keyvalue_seq', (select max(_KeyValue_key) from MGI_KeyValue))
;

select setval('mgi_property_seq', (select max(_Property_key) from MGI_Property))
;

select setval('mgi_note_seq', (select max(_Note_key) from MGI_Note))
;

EOSQL

#
# Run the classifier
#

echo 'Running processPredicted.sh'  | tee -a ${LOG_DIAG}
${GXDHTLOAD}/bin/processPredicted.sh >> ${LOG_DIAG}
STAT=$?
checkStatus ${STAT} "${GXDHTLOAD}/bin/processPredicted.sh"

#
# run postload cleanup and email logs
#
shutDown

