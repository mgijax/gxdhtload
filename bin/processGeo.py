#!/usr/local/bin/python

'''
#
# processGeo.py
#
#	See http://mgiwiki/mediawiki/index.php/sw:Gxdhtload
#
# Usage:
#       processGeo.py
#
# History:
#
# sc   09/30/2016
#       - created TR12370
#
'''
import db
import loadlib
import os
import string
import Set
import sys

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
# GEO experiment file
inFileName = os.environ['GEO_XML_FILE']
fpInFile = None

# QC file and descriptor
qcFileName = os.environ['QCFILE_NAME']
fpQcFile = None

# Property BCP File
propertyFileName = os.environ['GEO_PROPERTY_BCP']
fpPropertyBCP = None

# GEO report file
rptFileName = os.environ['GEO_RPT_FILE']
fpRptFile = None

# Number or experiments in GEO xml file
expCount = 0

# Number of experiments in the db whose pubmed IDs were updated
updateExpCount = 0

# GEO experiments NOT in the database
notInDbList = []

# PubMed property values
pubmedPropKey = 20475430
propTypeKey = 1002

# Experiment MGIType key
mgiTypeKey = 42

# GEO Logical DB
geoLdbKey = 190

# parse xml file int this
# {geoID:[pubmed1, ... pubmedn], ...}
geoPubMedDict = {}


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
    global fpQcFile, pubMedByExptDict, geoIdToKeyDict
    global fpPropertyBcp, nextPropKey
    global xmlDoc

    # create file descriptors
    try:
	fpQcFile = open(qcFileName, 'a')
    except:
	 print 'Cannot create %s' % qcFileName
    try:
        fpPropertyBcp = open(propertyFileName, 'w')
    except:
         print 'Cannot create %s' % propertyFileName

    db.useOneConnection(1)

    # get next primary key for the Property table
    results = db.sql('select max(_Property_key) + 1 as maxKey from MGI_Property', 'auto')
    if results[0]['maxKey'] == None:
        nextPropKey = 1000
    else:
        nextPropKey  = results[0]['maxKey']

    # create the pubmed ID property lookup by GEO experiment
    # get all experiments with 2ndary GEO IDs, and their pubMed IDs if they have them
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
    print 'in database'
    for x in pubMedByExptDict:
	print '%s %s%s' % (x, pubMedByExptDict[x], CRT)
    db.useOneConnection(0)
    
    parseAll()
    return

#
# Purpose: parse a geo file
# Returns: 
# Assumes: Nothing
# Effects: 
# Throws: Nothing
#

def parseFile(fpInFile):
    global geoPubMedDict, expCount
    expFound = 0
    gseId = ''
    pubMedIds = []
    line = string.lstrip(fpInFile.readline())
    # don't strip if just newline so blank lines won't stop the loop
    if line != '\n':
	 line = string.strip(line)

    while line:
	if line.find('<DocumentSummary ') == 0:
	    expFound = 1
	    expCount += 1
	elif line.find('</DocumentSummary>') == 0:
	    expFound = 0
	#print 'expFound: %s' % expFound
	if expFound:
	    if line.find('<Accession') == 0:
		id =  line.split('>')[1].split('<')[0]
		if id.find('GSE') == 0:
		    gseId = id
		    #print 'found gseId: %s' % gseId
	    elif line.find('<PubMedIds>') == 0:
		#print 'found open PubMedIds tag'
		line = fpInFile.readline()
		if line != '\n':
		    line = string.strip(line)
		while line.find('</PubMedIds>') == -1:
		    #print 'line in while: %s' % line
		    pubMedIds.append(line.split('>')[1].split('<')[0])
		    line = fpInFile.readline()
		    if line != '\n':
			line = string.strip(line)
	else:
	    if gseId != '' and pubMedIds != []:
		#print 'adding pubMed ID: %s to GEO id: %s' % (pubMedIds, gseId)
		geoPubMedDict[gseId] = pubMedIds
		gseId = ''
		pubMedIds = []
	line = fpInFile.readline()
	# don't strip if just newline so blank lines won't stop the loop
	if line != '\n':
	     line = string.strip(line)
    return

#
# Purpose: Loops through all input files sending them to parser
# Returns: 
# Assumes: Nothing
# Effects: 
# Throws: Nothing
#
def parseAll():
    fpInFile = None
    for file in string.split(os.environ['ALL_FILES']):
	try:
	    fpInFile = open(file, 'r')
	except:
	    print '%s does not exist' % inFileName
	parseFile(fpInFile)
	fpInFile.close()
    return
#
# Purpose: parse input file, QC, create bcp files
# Returns: 1 if file can be read/processed correctly, else 0
# Assumes: Nothing
# Effects: Creates files in the file system
# Throws: Nothing
#

def process():
    global nextPropKey
    global updateExpCount, notInDbList

    propertyUpdateTemplate = "#====#%s%s%s#=#%s#=====#%s%s%s#==#%s#===#%s%s%s%s%s%s%s%s%s" % (TAB, propTypeKey, TAB, TAB, TAB, mgiTypeKey, TAB, TAB, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT )
    for geoId in geoPubMedDict:
	# 
	# update pubmed ID properties, if this ID already in the database
	# if not in DB report it
	#
	if geoId not in pubMedByExptDict:
	    notInDbList.append(geoId)
	    continue
	else:
	    # get the list of pubmed Ids for this expt in the database
	    dbBibList = pubMedByExptDict[geoId]
	    geoBibList = geoPubMedDict[geoId]

	    # get the set of incoming pubmed IDs not in the database
	    newSet = set(geoBibList).difference(set(dbBibList))

	    # if we have new pubmed IDs, add them to the database
	    if newSet:
		updateExpKey = geoIdToKeyDict[geoId]

		# get next sequenceNum for this expt's pubmed ID in the database
		results = db.sql('''select max(sequenceNum) + 1 as nextNum
		    from MGI_Property p
		    where p._Object_key =  %s
		    and p._PropertyTerm_key = 20475430
		    and p._PropertyType_key = 1002''' % updateExpKey, 'auto')

		nextSeqNum = results[0]['nextNum']
		if nextSeqNum == None:
		    nextSeqNum = 1
		updateExpCount += 1
		for b in newSet:
		    toLoad = propertyUpdateTemplate.replace('#=#', str(pubmedPropKey)).replace('#==#', str(b)).replace('#===#', str(nextSeqNum)).replace('#====#', str(nextPropKey)).replace('#=====#', str(updateExpKey))
		    
		    fpPropertyBcp.write(toLoad)
		    nextSeqNum += 1
		    nextPropKey += 1
    return
	    
#
# Purpose: Writes statistics to the QC file
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: Closes files
# Throws: Nothing
#

def reportStats():
    fpQcFile.write('%sGEO Update Report%s' % (CRT, CRT))
    fpQcFile.write('--------------------------------------------------%s' % CRT)
    fpQcFile.write('Number of GEO experiments in DB: %s%s' % (len(pubMedByExptDict), CRT))
    fpQcFile.write('Number of GEO experiments in the input (at GEO): %s%s' % (expCount, CRT))
    fpQcFile.write('Number of GEO experiments with PubMed ID(s)in the input: %s%s' % (len(geoPubMedDict), CRT))
    fpQcFile.write('Number of GEO experiments updated in DB: %s%s' % (updateExpCount, CRT))

    fpQcFile.write('Number of GEO experiments not in DB: %s%s' % (len(notInDbList), CRT))

    fpQcFile.write('%sList of GEO experiments not in DB: %s' % (CRT, CRT))
    fpQcFile.write(string.join(notInDbList, CRT))
	    
    return

#
# Purpose: Close file descriptors
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: Closes files
# Throws: Nothing
#
def closeFiles():

    fpQcFile.close()
    fpPropertyBcp.close()

    return
#
# main
#

initialize()
process()
reportStats()
closeFiles()

