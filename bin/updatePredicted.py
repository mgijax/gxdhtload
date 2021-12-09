#
#  Purpose:
#
#  1. input:  ${PREDICTED_EXPERIMENT}
#
#  2. update gxd_htexperiment._evaluationstate_key
#
#  3. update gxd_htexperiment.confidence
#

import sys
import os
import db
import mgi_utils
import loadlib

db.setTrace()

predicted_yes_key = None
predicted_no_key = None

results = db.sql('''select _term_key, term 
        from voc_term 
        where term in ('Predicted Yes', 'Predicted No')''', 'auto')

for r in results:
    term = r['term']
    if term == 'Predicted Yes':
        predicted_yes_key = r['_term_key']
    else:
        predicted_no_key = r['_term_key']
        
print('predicted_yes_key: %s' % predicted_yes_key)
print('predicted_no_key: %s' % predicted_no_key)

experimentTable = 'GXD_HTExperiment'
userKey = 1626  # geo gxdhtload

outputDir = os.getenv('OUTPUTDIR')

updateSql = '''update gxd_htexperiment set _evaluationstate_key = %s, confidence = %s where _experiment_key = %s;\n'''
allUpdateSql = ''

inFile = open(os.getenv('PREDICTED_EXPERIMENT'), 'r')
lineNum = 0
for line in inFile.readlines():

        if lineNum == 0:
                lineNum += 1
                continue

        lineNum += 1
        tokens = line[:-1].split('|')

        geoID = tokens[0]
        predClass = tokens[1]
        confidence = tokens[2]
        evalStateKey = predicted_no_key
        if predClass == 'Yes':
            evalStateKey = predicted_yes_key
               
        exptKey = db.sql('''select _object_key from ACC_Accession where accid = '%s' and preferred = 1'''% (geoID), 'auto')[0]['_object_key']

        allUpdateSql += updateSql % (evalStateKey, confidence, exptKey)

inFile.close()
print(allUpdateSql)
# run the gxd_experiment update statements
db.sql(allUpdateSql, None)
db.commit()
