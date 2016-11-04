#!/usr/local/bin/python

'''
#
# flagExperiments.py
#
#	See http://mgiwiki/mediawiki/index.php/sw:Gxdhtload
#
# Usage:
#       flagExperiments.py
#
# History:
#
# sc   10/28/2016
#       - created TR12370
#
# TO DO - find experiments with multiple pubmed IDs that are selected for Expression
'''
import db
import loadlib
import os
import string
import Set
import sys

db.setTrace()
db.setAutoTranslate(False)
db.setAutoTranslateBE(False)

TAB = '\t'
CRT = '\n'

# today's date for modification date
loadDate = loadlib.loaddate

# user updating the database records, gxdhtload
userKey = 1561 

# state to update to
maybe = 20225944

# set of experiments in MGI with pubmed IDs selected for expression organized by
# experiment
# {exptKey:[list of pubMedIds], ...}
updateDict = {}

# updated experiments
updateList = []

# QC file and descriptor
qcFileName = os.environ['QCFILE_NAME']
fpQcFile = None

#
# Purpose:  Open file descriptors, get next primary keys, create lookups
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: Copies & opens files, read a database
# Throws: Nothing
#
def initialize():
    global updateDict, fpQcFile

    # create file qc descriptor in append mode
    try:
        fpQcFile = open(qcFileName, 'a')
    except:
         print 'Cannot create %s' % qcFileName

    db.useOneConnection(1)

    # get all pubmed experiment properties for 'not evaluated' experiments
    db.sql('''select e._Experiment_key, a.accid as exptId, p.value as pubMedId
	into temporary table pmProp
        from GXD_HTExperiment e, ACC_Accession a, MGI_Property p
        where e._EvaluationState_key = 20225941 
        and e._Experiment_key = a._Object_key
        and a._MGIType_key = 42
        and a._LogicalDB_key = 189
        and a.preferred = 1
        and e._Experiment_key = p._Object_key
        and p._PropertyTerm_key = 20475430
        and p._PropertyType_key = 1002''', None)
    db.sql('''create index idx1 on pmProp(pubMedId)''', None)

    # get set of experiments with pubMed IDs in MGI
    # experiments that have at least one pubmed ID in MGI will be updated
    db.sql('''select p.*, a._Object_key as _Refs_key
	into temporary table inMGI
	from pmProp p, ACC_Accession a
	where p.pubMedId = a.accid
	and a._LogicalDB_key = 29
	and a._MGIType_key = 1 ''', None)
    db.sql('''create index idx2 on inMGI(_Refs_key)''', None)

    # get the set of experiments with pubmed IDs selected for expression
    results = db.sql('''select p._Experiment_key, p.exptId, p.pubMedId, p._Refs_key
	from BIB_Dataset_Assoc bd, inMGI p
	where bd._DataSet_key = 1004
	and bd._Refs_key = p._Refs_key''', 'auto')

    # organize by experiment
    for r in results:
	exptKey = r['_Experiment_key']
	exptId = r['exptId']
	pubMedId = r['pubMedId']
	key = '%s|%s' % ( exptKey, exptId)
	if exptKey not in updateDict:
	    updateDict[key] = []
	updateDict[key].append(pubMedId)

    return

# Purpose: parse input file, QC, create bcp files
# Returns: 1 if file can be read/processed correctly, else 0
# Assumes: Nothing
# Effects: Creates files in the file system
# Throws: Nothing
#

def process():
    global updateList
    print 'in process'
    for key in updateDict:
        pubMedList = updateDict[key]
        exptKey, exptId = string.split(key, '|')
	updateList.append('%s%s%s%s' % (exptId, TAB, string.join(pubMedList, ', '), CRT))
        print 'Updating %s %s with PubMed IDs %s' %  (exptKey, exptId, pubMedList)
	db.sql('''Update GXD_HTExperiment
		set _EvaluationState_key = %s,
		evaluated_date = '%s',
		_EvaluatedBy_key = %s,
		modification_date = '%s',
		_ModifiedBy_key = %s
		where _Experiment_key = %s''' % (maybe, loadDate, userKey, loadDate, userKey, exptKey), None)
    db.commit()
    return


#
# Purpose: Writes statistics to the QC file
#
def reportStats():
    if len(updateList):
	fpQcFile.write("%sExperiments Flagged as 'maybe'%s" % (CRT, CRT))
	fpQcFile.write('--------------------------------------------------%s' % CRT)
	for line in updateList: 
	     fpQcFile.write(line)
	fpQcFile.write('%sTotal: %s%s%s' % (CRT, len(updateList), CRT, CRT))
#
# main
#

print 'init'
initialize()
print 'process'
process()
reportStats()

db.useOneConnection(0)
