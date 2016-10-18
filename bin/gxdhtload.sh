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
# get GEO pubmed IDs
#

###############################################################################
#
# following from NCBI Eutils documentation:
#
###############################################################################
# query_key: Query key. This integer specifies which of the UID lists attached 
#  to the given Web Environment will be used as input to ESummary. Query keys 
#  are obtained from the output of previous ESearch, EPost or ELink calls. 
#  The query_key parameter must be used in conjunction with WebEnv.

# WebEnv: Web Environment. This parameter specifies the Web Environment that 
#  contains the UID list to be provided as input to ESummary. Usually this 
#  WebEnv value is obtained from the output of a previous ESearch, EPost or 
#  ELink call. The WebEnv parameter must be used in conjunction with query_key.
#   esummary.fcgi?db=protein&query_key=<key>&WebEnv=<webenv string>

# db=gds: from example IV: https://www.ncbi.nlm.nih.gov/geo/info/geo_paccess.html#Programs

# term=GSE[ETYP]+AND+Mus[ORGN] : from above example

# retstart= : Sequential index of the first UID in the retrieved set to be shown
#   in the XML output (default=0, corresponding to the first record of the 
#  entire set). This parameter can be used in conjunction with retmax to 
#  download an arbitrary subset of UIDs retrieved from a search.

# retmax= : maximum of 100,000 records. To retrieve more than 100,000 UIDs,
#  submit multiple esearch requests while incrementing the value of retstart
#  (see https://www.ncbi.nlm.nih.gov/books/NBK25498/#chapter3.Application_3_Retrieving_large)

# usehistory=y : ESearch will post the UIDs resulting from the search operation
#   onto the History server so that they can be used directly in a subsequent 
#  E-utility call.

# datetype=pdat : Type of date used to limit a search. The allowed values vary 
#  between Entrez databases, but common values are 'mdat' (modification date), 
#  'pdat' (publication date) and 'edat' (Entrez date).

# reldate : When reldate is set to an integer n, the search returns only those 
#  items that have a date specified by datetype within the last n days.
################################################################################

#
# step one get query key and web env and query count
#
wget  -O $GEO_UID_FILE "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=gds&term=GSE[ETYP]+AND+Mus[ORGN]&retmax=300000&usehistory=y&datetype=pdat&reldate=36500"

QUERY_KEY=`cat ${GEO_UID_FILE} | grep '<QueryKey>' | cut -d'>' -f8 | cut -d'<' -f1`
WEB_ENV=`cat ${GEO_UID_FILE} | grep '<WebEnv>' | cut -d'>' -f10 | cut -d'<' -f1`
GEO_COUNT=`cat ${GEO_UID_FILE} | grep '<Count>' | cut -d'>' -f2 | cut -d'<' -f1`
echo "QUERY_KEY: $QUERY_KEY"
echo "WEB_ENV: $WEB_ENV"
echo "GEO_COUNT: $GEO_COUNT"

# number of experiments to retrieve from the HISTORY server in one batch
retrieve_max=5000

# counter for batch file extension
fileCount=1

# starting point for retrieval from history server
retrieve_start=0

#
# step two get experiments from the history server
#

# first  delete old files
rm  ${GEO_XML_FILE}.*

# Loop grabbing retrieve_max experiments at a time
while [ $retrieve_start -lt $GEO_COUNT ]
do
    echo "fileCount: $fileCount"
    echo "retrieve_start: $retrieve_start"

    wget -O ${GEO_XML_FILE}.${fileCount} "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=gds&version=2.0&query_key=${QUERY_KEY}&WebEnv=${WEB_ENV}&retstart=$retrieve_start&retmax=$retrieve_max"
    fileCount=`expr $fileCount + 1`
    retrieve_start=`expr $retrieve_start + $retrieve_max`
done

ALL_FILES=`ls /data/loads/sc/mgi/gxdhtload/input/geo.xml.*`
export ALL_FILES
echo "ALL_FILES: ${ALL_FILES}"

# step three, parse experiments/load new PubMedIds
echo 'Running processGeo.py'  | tee -a ${LOG_DIAG}
${GXDHTLOAD}/bin/processGeo.py #>> ${LOG_DIAG}
STAT=$?
checkStatus ${STAT} "${GXDHTLOAD}/bin/processGeo.py"

# BCP PubMed properties from GEO
TABLE=MGI_Property

if [ -s "${OUTPUTDIR}/${GEO_PROPERTY_FILENAME}" ]
then

    echo "" >> ${LOG_DIAG}
    date >> ${LOG_DIAG}
    echo "BCP data into ${TABLE}"  >> ${LOG_DIAG}

    # BCP new data
    ${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ${TABLE} ${OUTPUTDIR} ${GEO_PROPERTY_FILENAME} ${COLDELIM} ${LINEDELIM} >> ${LOG_DIAG}
fi

#
# run postload cleanup and email logs
#
shutDown

