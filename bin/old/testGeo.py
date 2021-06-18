'''
#
# testGeo.py
#
#	See http://mgiwiki/mediawiki/index.php/sw:Gxdhtload
#
# Usage:
#       testGeo.py
#
# History:
#
# sc   10/16/2016
#       - created TR12370
#
'''
import os
import sys
import Set
import db
import loadlib

TAB = '\t'
CRT = '\n'

# today's date
loadDate = loadlib.loaddate

# user creating the database records, gxdhtload
# used for record createdBy and modifiedBy
userKey = 1561 

#
# File Descriptors:
#
# PubMed IDs from GEO
inFileName = '/mgi/all/wts_projects/12300/12370/testing/US111/GeoMouse.mouse_10.17.16.txt'
fpInFile = None

# QC file and descriptor
qcFileName = '/mgi/all/wts_projects/12300/12370/testing/US111/US111.rpt'
fpQcFile = None

# Number or experiments in GEO file
expCount = 0

# GEO experiments NOT in the database
notInDbList = []

# GEO experiments in GXD not in GEO
notInGeoList = []

# PubMed IDs in GEO not in GXD
# {geoID:[pubMed1, ...], ...}
pubMedNotInDbDict = {}

geoPubMedDict = {}

# PubMed property values
pubmedPropKey = 20475430
propTypeKey = 1002

# Experiment MGIType key
mgiTypeKey = 42

# GEO Logical DB
geoLdbKey = 190

# pubmed properties by GEO ID in the db
# {GEO ID: [pubmedId1, ..., pubmedIdn], ...}
pubMedByExptDict = {}

# GEO Ids in the database mapped to their experiment keys
# {geoID:key, ...}
geoIdToKeyDict = {}

#
# Purpose:  Open file descriptors, get next primary keys, create lookups
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: Copies & opens files, read a database
# Throws: Nothing
#
def initialize():
    global fpQcFile, fpInFile, pubMedByExptDict, geoIdToKeyDict

    # create file descriptors
    try:
        fpInFile = open(inFileName, 'r')
    except:
         print('Cannot create %s' % inFileName)
    # create file descriptors
    try:
        fpQcFile = open(qcFileName, 'w')
    except:
         print('Cannot create %s' % qcFileName)
    db.useOneConnection(1)

    # create the pubmed ID property lookup by GEO experiment
    # get all experiments with 2ndary GEO IDs, and their pubMed IDs if they 
    # have them
    db.sql('''select p._Object_key, p.value
        into temporary table pubMedIds 
        from MGI_Property p
        where p._PropertyTerm_key = %s
        and p._PropertyType_key = %s''' % (pubmedPropKey, propTypeKey), None)
    db.sql('''create index idx1 on pubMedIds(_Object_key)''', None)
    db.sql('''select e._Experiment_key, a.accid
        into temporary table exp
        from GXD_HTExperiment e, ACC_Accession a
        where e._Experiment_key = a._Object_key
        and a._MGIType_key = %s
        and a._LogicalDB_key = %s''' % (mgiTypeKey, geoLdbKey), None)
    db.sql('''create index idx2 on exp(_Experiment_key)''', None)
    results = db.sql('''select e._Experiment_key, e.accid, p.value
        from exp e
        left outer join pubMedIds p on (e._Experiment_key = p._Object_key)''', 'auto')
    for r in results:
        key = r['_Experiment_key']
        accid = r['accid'] 
        value = r['value']

        geoIdToKeyDict[accid] = key

        if accid not in pubMedByExptDict:
            pubMedByExptDict[accid] = []
        if value != None:
            pubMedByExptDict[accid].append(value)
    
    db.useOneConnection(0)
    
    return

def parseFile():
    global geoPubMedDict
    for line in fpInFile.readlines():
        geoId, pubMedIds = list(map(str.strip, str.split(line, TAB)))
        #print '%s:"%s"' % (geoId, pubMedIds)
        if pubMedIds != '':
            geoPubMedDict[geoId] = str.split(pubMedIds, ';')
            #print geoPubMedDict[geoId]

#
# Purpose: parse input file, QC, create bcp files
# Returns: 1 if file can be read/processed correctly, else 0
# Assumes: Nothing
# Effects: Creates files in the file system
# Throws: Nothing
#

def process():
    global pubMedNotInDbDict
    
    for geoId in geoPubMedDict:
        if geoId in pubMedByExptDict:
            geoPubMed = set(geoPubMedDict[geoId])
            mgdPubMed = set(pubMedByExptDict[geoId])
            if mgdPubMed == None:
                mgdPubMed = set([])
            # find pubmed IDs in GEO not in GXD
            difference = geoPubMed.difference(mgdPubMed)
            print('geoId: %s' % geoId)
            print('geoPubMed: %s' % geoPubMed)
            print('mgdPubMed: %s' % mgdPubMed)
            print('difference: %s' % difference)
            print('\n')
            if difference:
                pubMedNotInDbDict[geoId] = list(difference)
    for g in pubMedNotInDbDict:
        print('geoId: %s difference: %s' % (g, pubMedNotInDbDict[g]))
def reportStats():
    fpQcFile.write("%sPubMed IDs in GEO (Connie's File) that are not in GXD%s" % (CRT, CRT))
    fpQcFile.write('--------------------------------------------------%s' % CRT)
    for g in pubMedNotInDbDict:
         fpQcFile.write('%s%s%s%s' % (g, TAB, pubMedNotInDbDict[g], CRT))
    return

#
# Purpose: Close file descriptors
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: Closes files
# Throws: Nothing
#
def closeFiles():
    fpInFile.close()
    fpQcFile.close()

    return
#
# main
#

initialize()
parseFile()
process()
reportStats()
closeFiles()
