#!/bin/sh

#
#
# processPredicted.sh
#
# The purpose of this script is to process the GXD Classifier  predictions
#
#
#  Purpose:
#
#  1. makePredicted.py - select experiments where evaluation state = "Not Evaluated" (20225941)
#
#  2.  predict.py (lib/python_anaconda)
#           . using NOT_EVALUATED_EXPERIMENT, process the predictions (PREDICTED_EXPERIMENT)
#           . creates input file for relevance classifier/predicter (.../lib/python_anaconda/predict.py)
#
#  3. updatePredicted.py
#           . using PREDICTED_EXPERIMENT,  update gxd_htexperiment._evaluation_state_key to Predicted Yes or Predicted No.
#

cd `dirname $0` 

COMMON_CONFIG=../geo_htload.config

# 
# Make sure the common configuration file exists and source it. 
#
if [ -f ${COMMON_CONFIG} ]
then
    . ${COMMON_CONFIG}
else
    echo "Missing configuration file: ${COMMON_CONFIG}"
    exit 1
fi

if [ -f ${ANACONDAPYTHON} ]
then
    PYTHON=${ANACONDAPYTHON}; export PYTHON
    PYTHONPATH=${PYTHONPATH}:${ANACONDAPYTHONLIB}; export PYTHONPATH
else
    echo "Missing configuration file: ${ANACONDAPYTHON}"
    exit 1
fi

LOG=${LOG_EXPERIMENT}
rm -rf ${LOG_EXPERIMENT}
>>${LOG_EXPERIMENT}

date >> ${LOG_EXPERIMENT} 2>&1
echo 'PYTHON', $PYTHON  >> ${LOG_EXPERIMENT} 2>&1
echo 'PYTHONPATH', $PYTHONPATH  >> ${LOG_EXPERIMENT} 2>&1

date >> ${LOG_EXPERIMENT} 2>&1
echo "Running makePredicted.py" >> ${LOG_EXPERIMENT} 2>&1
${PYTHON} makePredicted.py  >> ${LOG_EXPERIMENT} 2>&1

date >> ${LOG_EXPERIMENT} 2>&1
echo "Running predict.py" >> ${LOG_EXPERIMENT} 2>&1
rm -rf ${PREDICTED_EXPERIMENT}

${ANACONDAPYTHON} ${ANACONDAPYTHONLIB}/predict.py --sampledatalib ${ANACONDAPYTHONLIB}/htMLsample.py -m ${ANACONDAPYTHONLIB}/gxdhtclassifier.pkl -p removeURLs -p textTransform_all -p stem  ${NOT_EVALUATED_EXPERIMENT} > ${PREDICTED_EXPERIMENT}

date >> ${LOG_EXPERIMENT} 2>&1
echo "Running updatePredicted.py" >> ${LOG_EXPERIMENT} 2>&1
${PYTHON} updatePredicted.py  >> ${LOG_EXPERIMENT} 2>&1

date >> ${LOG_EXPERIMENT} 2>&1
