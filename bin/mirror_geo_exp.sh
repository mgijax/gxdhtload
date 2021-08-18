#!/bin/sh
#
#  mirror_geo_expt.sh
###########################################################################
#
#  Purpose:
#       This script mirrors the experiment input files for the GEO gxdhtload
#
#  Usage=mirror_geo_expt.sh
#
# History:
#
# sc    06/17/2021 - WTS2-431
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
LOG=${GEO_LOG_FILE}
rm -rf ${LOG}
touch ${LOG}

#
# Create the download directory if it doesn't exist.
#
if [ ! -d ${GEO_DOWNLOADS} ]
then
    mkdir -p ${GEO_DOWNLOADS}
fi

# how far to go back to fetch experiments in days
reldate=${EXPT_DOWNLOAD_DAYS}

#
# step one get query key, web env and query count
#
if [ -z ${reldate} ]
then
    echo "empty"
    wget  -a ${LOG} -O $GEO_UID_FILE "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=gds&term=GSE[ETYP]+AND+Mus[ORGN]&retmax=300000&usehistory=y&datetype=pdat"
else
    echo "not empty"
    wget  -a ${LOG} -O $GEO_UID_FILE "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=gds&term=GSE[ETYP]+AND+Mus[ORGN]&reldate=${reldate}&retmax=300000&usehistory=y&datetype=pdat"
fi

QUERY_KEY=`cat ${GEO_UID_FILE} | grep '<QueryKey>' | cut -d'>' -f8 | cut -d'<' -f1`
WEB_ENV=`cat ${GEO_UID_FILE} | grep '<WebEnv>' | cut -d'>' -f10 | cut -d'<' -f1`
GEO_COUNT=`cat ${GEO_UID_FILE} | grep '<Count>' | cut -d'>' -f2 | cut -d'<' -f1`
#echo "QUERY_KEY: $QUERY_KEY"
#echo "WEB_ENV: $WEB_ENV"
#echo "GEO_COUNT: $GEO_COUNT"

# number of experiments to retrieve from the HISTORY server in one batch
retrieve_max=5000

# counter for batch file extension
fileCount=1

# starting point for retrieval from history server
retrieve_start=0

#
# step two get experiments from the history server
#

# first  delete old files from the archive directory
rm -f ${GEO_DOWNLOADS_ARCHIVE}/${EXPT_XML_FILE}.*

# then move the last processed files to the archive directory
mv ${GEO_DOWNLOADS}/${EXPT_XML_FILE}.* ${GEO_DOWNLOADS_ARCHIVE}

# Loop grabbing retrieve_max experiments at a time
while [ $retrieve_start -lt $GEO_COUNT ]
do
    echo "fileCount: $fileCount"
    echo "retrieve_start: $retrieve_start"

    wget -a ${LOG} -O ${GEO_XML_FILE}.${fileCount} "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=gds&version=2.0&query_key=${QUERY_KEY}&WebEnv=${WEB_ENV}&retstart=$retrieve_start&retmax=$retrieve_max&api_key=${EUTILS_API_KEY}"
    fileCount=`expr $fileCount + 1`
    retrieve_start=`expr $retrieve_start + $retrieve_max`
done

ALL_FILES=`ls ${GEO_DOWNLOADS}/geo.xml.*`
export ALL_FILES
