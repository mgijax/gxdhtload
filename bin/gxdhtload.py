#!/usr/local/bin/python

'''
#
# gxdhtload.py
#
#	See http://mgiwiki/mediawiki/index.php/sw:Gxdhtload
#
# Usage:
#       gxdhtload
#
# History:
#
# sc   08/25/2016
#       - created TR12370
#
'''
import db
import loadlib
import accessionlib
import types
import re
import os
import string
import Set
import simplejson as json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

TAB = '\t'
CRT = '\n'

# Array Express primary ID prefix for GEO experiments
AEGEOPREFIX = 'E-GEOD-'

# GEO primary ID prefix for experiments
GEOPREFIX = 'GSE'

# today's date
loadDate = loadlib.loaddate

# user creating the database records, gxdhtload
# used for record createdBy and modifiedBy
userKey = 1561 

#
# For ACC_Accession:
#
# Experiment MGIType key
mgiTypeKey = 42

# ArrayExpress LogicalDB key
aeLdbKey = 189

# GEO LogicalDB key
geoLdbKey = 190

isPreferred = 1
notPreferred = 0
private = 0

#
# For GXD_HTExperimentVariable:
#
# 'Not Curated' from 'GXD HT Variables' (vocab key=122)
exptVariableTermKey = 20475439 

# For GXD_HTExperiment:
# 'Not Evaluated' from 'GXD HT Evaluation State' (vocab key = 116) 
evalStateTermKey = 20225941 

# 'Not Curated' from GXD HT Curation State' (vocab key=117)
curStateTermKey = 20475421

# 'Not Curated' from 'GXD HT Study Type' (vocab key=124)
studyTypeTermKey = 20475461 

# 'Not Resolved' from GXD HT Experiment Type' (vocab key=121)
exptTypeNRKey = 20475438

# 'ArrayExpress' from 'GXD HT Source' (vocab key = 119)
sourceKey = 20475431  

# value for evaluation date, initial curated date and last curated date
# evaluated by, initial curated by, last curated by
nullValue = '|'

#
# File Descriptors:
#
# ArrayExpress file and descriptor
inFileName = os.environ['INFILE_NAME']
fpInFile = None

# QC file and descriptor
qcFileName = os.environ['QCFILE_NAME']
fpQcFile = None

# BCP files
experimentFileName = os.environ['EXPERIMENT_BCP']
fpExperimentBcp = None

accFileName = os.environ['ACC_BCP']
fpAccBcp = None

variableFileName = os.environ['VARIABLE_BCP']
fpVariableBcp =  None

propertyFileName = os.environ['PROPERTY_BCP']
fpPropertyBcp = None

# Number or experiments in AE json file
expCount = 0
# Number experiments loaded
loadedCount = 0
#Number of experiments already in the database
skippedCount = 0

# cache of IDs and counts in the input
# idDict = {primaryID:count, ...}
idDict = {}

# records with no id
noIdList = []

# AE IDs that have non integer sample count
# looks like {primaryID:sampleCount, ...}
invalidSampleCountDict = {}

# AE IDs that have invalid release and update dates
# looks like {primaryID:date, ...}
invalidReleaseDateDict = {}
invalidUpdateDateDict = {}

# database lookups

# AE IDs in the database
primaryIDList = []

# raw experiment types mapped to controlled vocabulary keys
exptTypeTransDict = {}
#
# Purpose:  Open file descriptors, get next primary keys, create lookups
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: Copies & opens files, read a database
# Throws: Nothing
#
def initialize():
    global fpInFile, fpQcFile, fpExperimentBcp, fpAccBcp, fpVariableBcp, fpPropertyBcp
    global jFile, nextExptKey, nextAccKey, nextExptVarKey, nextPropKey

    # create file descriptors
    try:
	fpInFile = open(inFileName, 'r')
    except:
	print '%s does not exist' % inFileName
    try:
	fpQcFile = open(qcFileName, 'w')
    except:
	 print 'Cannot create %s' % qcFileName

    try:
        fpExperimentBcp = open(experimentFileName, 'w')
    except:
        print 'Cannot create %s' % experimentFileName

    try:
        fpAccBcp = open(accFileName, 'w')
    except:
        print 'Cannot create %s' % accFileName

    try:
        fpVariableBcp = open(variableFileName, 'w')
    except:
        print 'Cannot create %s' % variableFileName 

    try:
        fpPropertyBcp = open(propertyFileName, 'w')
    except:
        print 'Cannot create %s' % propertyFileName

    jFile = json.load(fpInFile)

    db.useOneConnection(1)

    # get next primary key for the Experiment table    
    results = db.sql('select max(_Experiment_key) + 1 as maxKey from GXD_HTExperiment', 'auto')
    if results[0]['maxKey'] == None:
	nextExptKey = 1000
    else:
	nextExptKey  = results[0]['maxKey']

    # get next primary key for the Accession table
    results = db.sql('select max(_Accession_key) + 1 as maxKey from ACC_Accession', 'auto')
    nextAccKey  = results[0]['maxKey']

    # get next primary key for the ExperimentVariable table
    results = db.sql('select max(_ExperimentVariable_key) + 1 as maxKey from GXD_HTExperimentVariable', 'auto')
    #print 'results: %s' % results[0]['maxKey']
    if results[0]['maxKey'] == None:
        nextExptVarKey = 1000
    else:
	nextExptVarKey  = results[0]['maxKey']
    #print 'nextExptKey: %s' % nextExptKey
    #print 'nextAccKey: %s ' % nextAccKey
    #print 'nextExptVarKey: %s' % nextExptVarKey

    # get next primary key for the Property table
    results = db.sql('select max(_Property_key) + 1 as maxKey from MGI_Property', 'auto')
    if results[0]['maxKey'] == None:
        nextPropKey = 1000
    else:
        nextPropKey  = results[0]['maxKey']
    print 'nextPropKey: %s' % nextPropKey

    # Create experiment ID lookup
    results = db.sql('''select accid 
	from ACC_Accession
	where _MGIType_key = 42
	and _LogicalDB_key = 189
	and preferred = 1''', 'auto')
    for r in results:
	primaryIDList.append(r['accid'])

    # Create experiment type translation lookup
    results = db.sql('''select badname, _Object_key
	from MGI_Translation
	where _TranslationType_key = 1020''', 'auto')
    for r in results:
	exptTypeTransDict[r['badname']] = r['_Object_key']

    db.useOneConnection(0)
    
    return

def checkPrimaryId(id):
    global idDict
    if id not in idDict:
        idDict[id] = 1
    else:
        idDict[id] += 1
    return

def checkInteger(rawText):
    if type(rawText) == types.IntType:
        return 1
    elif type(rawText) == types.StringType:
        for c in rawText:
            if not c.isdigit():
                return 0
        return 1
    return 0

def checkDate(rawText):
    if rawText.find(',') > -1:
        return 0
    # yyyy-mm-dd format
    ymd = re.compile('([0-9]{4})-([0-9]{2})-([0-9]{2})')
    ymdMatch = ymd.match(rawText)
    #print 'checkdate: %s' % rawText
    if ymdMatch:
        (year, month, day) = ymdMatch.groups()
        #print 'year: %s month: %s day %s' % (year, month, day)
        if (1950 <= int(year) <= 2050):
            if (1 <= int(month) <= 12):
                if (1 <= int(day) <= 31):
                    #print 'return 1'
                    return 1
    #print 'return 0'
    return 0

#
# Purpose: does QC
# Returns: 1 if qc error, else 0
# Assumes: Nothing
# Effects: 
# Throws: Nothing
#

def doQcChecks(primaryID, name, sampleCount, releasedate, lastupdatedate):
    hasError = 0
    if primaryID == '':
	noIdList.append('Name: %s' % name)
	# if no primary ID skip remaining checks
	return 1
    else:
	checkPrimaryId(primaryID)

    # check that sample is integer
    #print 'primaryID: %s sampleCount: %s isInt: %s' % (primaryID, sampleCount, checkInteger(sampleCount))
    if sampleCount and checkInteger(sampleCount)== 0:
	invalidSampleCountDict[primaryID] = sampleCount
	hasError = 1
    # check dates
    #print 'check releasedate'
    #print 'primaryID: %s' % primaryID
    if releasedate != '' and checkDate(releasedate) == 0:
	#print 'adding %s to invalidReleaseDateDict' % releasedate
	invalidReleaseDateDict[primaryID] = releasedate
	hasError = 1

    #print 'check lastupdatedate'
    if lastupdatedate != '' and checkDate(lastupdatedate) == 0:
	#print 'adding %s to invalidUpdateDateDict' % lastupdatedate
	invalidUpdateDateDict[primaryID]= lastupdatedate
	hasError = 1

    return hasError

#
# Purpose: calculate a GEO ID from and AE GEO ID
# Returns: GEO ID or empty string if not an AE GEO ID
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#

def calculateGeoId(primaryID):
    if primaryID.find(AEGEOPREFIX) == 0:
	return primaryID.replace(AEGEOPREFIX, GEOPREFIX)
    else:
	return ''
#
# Purpose: parse input file, QC, create bcp files
# Returns: 1 if file can be read/processed correctly, else 0
# Assumes: Nothing
# Effects: Creates files in the file system
# Throws: Nothing
#
def process():
    global propertiesDict, expCount, loadedCount, skippedCount, invalidSampleCountDict
    global invalidReleaseDateDict, invalidUpdateDateDict, noIdList
    global nextExptKey, nextAccKey, nextExptVarKey,nextPropKey
    
    for f in jFile['experiments']['experiment']:
        expCount += 1
	print 'Expt# %s' % expCount
	try:
            description = f['description']['text'] # experiment, one
	    if type(description) ==  types.ListType:
		description = description[0]
	    else: # could be NoneType
		description = None
	except:
    	    description = None
	print 'description: %s' % description
	# replace any escapes
	#description.replace('\\/', '//')
	print 'description type: %s' % type(description)
	#print 'description find escape: %s' % description.find('\\\/')
	#if description.find('C57BL') != -1:
	#    print 'found C57BL'
	try:
	    name = f['name'] # experiment and property, many; |-delim in both
	    if type(name) == types.ListType:
		name = '|'.join(name)
	except:
	    name = ''
	print 'name: %s' % name

	try:
	    primaryID = f['accession'] # accession
	except:
	    primaryID = ''
	print 'primaryID: %s' % primaryID
	
	try:
	    sampleCount = f['samples'] # property, one
	except:
	    sampleCount = ''
	print 'sampleCount: %s' % sampleCount

	try:
	    releasedate = f['releasedate'] # experiment, one
	except:
	    releasedate = ''
	print 'releasedate: %s' % releasedate

        try:
	    # experimentalfactor.name
	    expFactorSet = set()
            for e in f['experimentalfactor']: # property, many stored individ.		
		expFactorSet.add(e['name'])
	    expFactorList = list(expFactorSet)
        except:
            expFactorList = []
	print 'expFactorList: %s' % expFactorList

        try:
            lastupdatedate = f['lastupdatedate'] # experiment, one
        except:
            lastupdatedate = ''
	print 'lastupdatedate %s' % lastupdatedate

        try:
	    # provider.contact, need to remove exact dups
	    providerSet = set()
            for p in  f['provider']:
		providerSet.add(p['contact']) # property, many stored individually
	    providerList = list(providerSet)
        except:
            providerList = []
	print 'providerList: %s' % providerList

        try:
	    # experimenttype is string or list, property, many stored individ
	    if type( f['experimenttype']) != types.ListType:
		experimenttypeList = [ f['experimenttype']]
	    else:
		experimenttypeList =  f['experimenttype']
        except:
            experimenttypeList = []
	print 'experimenttypeList: %s' % experimenttypeList

	# pick first experiment type and translate it to populate the exptype key
	experiment = experimenttypeList[0] 
	if experiment not in exptTypeTransDict:
	    exptTypeKey = exptTypeNRKey # Not Resolved
	else:
	    exptTypeKey = exptTypeTransDict[experiment]

        try:
	# PubMed IDs - bibliography.accession
	    bibliographyList = []
	    #print 'type bibliography: %s bibliography: %s' %  (type(f['bibliography']),  f['bibliography'])
	    if type(f['bibliography']) == types.DictType: # dictionary
		 bibliographyList.append(f['bibliography']['accession'])
	    else: # ListType
		for b in f['bibliography']: # for each dict in the list
		    #print 'b: %s' % b
		    if 'accession' in b:
		        bibliographyList.append(b['accession'])
	except:
	    #print 'exception raised'
            bibliographyList = []
	print 'bibliographyList: %s' % (  bibliographyList)

	# 
	# skip if this ID already in the database
	#
	if primaryID in primaryIDList:
	    skippedCount += 1
	    continue

	prefixPartPrimary, numericPartPrimary = accessionlib.split_accnum(primaryID)
	#
	# Do QC checks
	# If there are errors, skip to the next experiment
	if doQcChecks(primaryID, name, sampleCount, releasedate, lastupdatedate):
	    continue

	# calculate secondary GEO ID for AE GEO IDs
	geoID = calculateGeoId(primaryID)

	# 
	# now write out to bcp files
	#
	loadedCount += 1

	# GXD_Experiment 
	# many optional nulls - create the insert string
        line = '%s%s%s%s' % (nextExptKey, TAB, sourceKey, TAB)
	if name != '':
	    line = line + name + TAB
	if description  != '' and description != None:
	    line = line + description + TAB
	else:
	    line = line + TAB
	if releasedate != '':
	    line = line + releasedate + TAB
	else:
	     line = line + TAB
	if lastupdatedate != '':
	    line = line + lastupdatedate + TAB
	else: 
	    line = line + TAB
	# evaluated_date is null
	line = line + TAB
	line = line + str(evalStateTermKey) + TAB
	line = line + str(curStateTermKey) + TAB
	line = line + str(studyTypeTermKey) + TAB
	line = line + str(exptTypeKey) + TAB
	# evalByKey, initialCurByKey, lastCurByKey, initialCurDate, lastCurDate all null
	line = line + TAB + TAB + TAB + TAB + TAB
	# created and modified by
	line = line + str(userKey) + TAB + str(userKey) + TAB
	# creation and modification date
	line = line + loadDate + TAB + loadDate + CRT
	fpExperimentBcp.write(line)
	#fpExperimentBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextExptKey, TAB, sourceKey, TAB, name, TAB, description, TAB, releasedate, TAB, lastupdatedate, TAB, TAB, evalStateTermKey, TAB, curStateTermKey, TAB, studyTypeTermKey, TAB, exptTypeKey, TAB, TAB, TAB, TAB, TAB, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT))

	# Primary Accession 
	fpAccBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextAccKey, TAB, primaryID, TAB, prefixPartPrimary, TAB, numericPartPrimary, TAB, aeLdbKey, TAB, nextExptKey, TAB, mgiTypeKey, TAB, private, TAB, isPreferred, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT ))
	nextAccKey += 1

	# Secondary Accession 
	if geoID != '':
	    prefixPartSecondary, numericPartSecondary = accessionlib.split_accnum(geoID)
	    fpAccBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextAccKey, TAB, geoID, TAB, prefixPartSecondary, TAB, numericPartSecondary, TAB, geoLdbKey, TAB, nextExptKey, TAB, mgiTypeKey, TAB, private, TAB, notPreferred, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT ))
	    nextAccKey += 1

	# Variable
	fpVariableBcp.write('%s%s%s%s%s%s' % (nextExptVarKey, TAB, nextExptKey, TAB, exptVariableTermKey, CRT))
	nextExptVarKey += 1

	# Properties 
	nextExptKey += 1

    print 'Number of experiments in the input: %s' % expCount
    print 'Number of experiments already in the database: %s' % skippedCount
    print 'Number of experiments loaded: %s' % loadedCount

    return

def writeQC():
    fpQcFile.write('GXD HT Raw Data Load QC%s%s%s' % (CRT, CRT, CRT))


    fpQcFile.write('Experiments with no Primary ID%s' % CRT)
    fpQcFile.write('--------------------------------------------------%s' % CRT)
    ct = 0
    for name in noIdList:
	ct += 1
	fpQcFile.write('%s%s' %  (name, CRT))
    fpQcFile.write('%sTotal: %s%s%s' % (CRT, ct, CRT, CRT))

    fpQcFile.write('Multiple Experiments with same AE ID%s' % CRT)
    fpQcFile.write('ID%sCount%s' % (TAB, CRT))
    fpQcFile.write('--------------------------------------------------%s' % CRT)
    ct = 0
    for id in idDict:
	if idDict[id] > 1:
	    ct += 1
	    fpQcFile.write('%s%s%d%s' %  (id, TAB, idDict[id], CRT))
    fpQcFile.write('%sTotal: %s%s%s' % (CRT, ct, CRT, CRT))

    fpQcFile.write('Experiments with Invalid Sample Count%s' % CRT)
    fpQcFile.write('ID%sSample Count%s' % (TAB, CRT))
    fpQcFile.write('--------------------------------------------------%s' % CRT)
    ct = 0
    for id in invalidSampleCountDict:
	ct += 1
	fpQcFile.write('%s%s%s%s' %  (id, TAB, invalidSampleCountDict[id], CRT))
    fpQcFile.write('\nTotal: %s%s%s' % (ct, CRT, CRT))

    #print 'invalid release date dict'
    #print invalidReleaseDateDict
    #print 'invalid update date dict'
    #print invalidUpdateDateDict
    fpQcFile.write('Experiments with Invalid Release Date%s' % CRT)
    fpQcFile.write('ID%sRelease Date%s' % (TAB, CRT))
    fpQcFile.write('--------------------------------------------------%s' % CRT)
    ct = 0
    for id in invalidReleaseDateDict:
        ct += 1
	fpQcFile.write('%s%s%s%s' %  (id, TAB, invalidReleaseDateDict[id], CRT))
    fpQcFile.write('\nTotal: %s%s%s' % (ct, CRT, CRT))

    fpQcFile.write('Experiments with Invalid Update Date%s' % CRT)
    fpQcFile.write('ID%sUpdate Date%s' % (TAB, CRT))
    fpQcFile.write('--------------------------------------------------%s' % CRT)
    ct = 0
    for id in invalidUpdateDateDict:
        ct += 1
        fpQcFile.write('%s%s%s%s' %  (id, TAB, invalidUpdateDateDict[id], CRT))
    fpQcFile.write('\nTotal: %s%s%s' % (ct, CRT, CRT))

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
    fpExperimentBcp.close()
    fpAccBcp.close()
    fpVariableBcp.close()
    fpPropertyBcp.close()

    return
#
# main
#

initialize()
process()
writeQC()
closeFiles()
