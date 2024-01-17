'''
#
# ae_htload.py
#
#	See http://mgiwiki/mediawiki/index.php/sw:Gxdhtload
#
# Usage:
#       ae_htload.py
#
# History:
#
# sc  12/29/2023
#       - created WTS2-545
#
'''
import os
import sys
import types
import re
import Set
import json
import db
import loadlib
import accessionlib

TAB = '\t'
CRT = '\n'

# the list of geo experiment Ids to load
expIdList = []

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

isPreferred = 1
notPreferred = 0
private = 0

# For GXD_HTExperimentVariable:
# 'Not Curated' from 'GXD HT Variables' (vocab key=122)
exptVariableTermKey = 20475439 

# For GXD_HTExperiment:
# 'Not Evaluated' from 'GXD HT Evaluation State' (vocab key = 116) 
defaultEvalStateTermKey = 20225941 

# 'No' from  'GXD HT Evaluation State' (vocab key = 116)
altEvalStateTermKey=20225943

# 'Not Done' from GXD HT Curation State' (vocab key=117)
curStateTermKey = 20475422

# 'Not Applicable' from 'GXD HT Curation State' (vocab key = 117)
altCurStateTermKey = 20475420

# 'Not Curated' from 'GXD HT Study Type' (vocab key=124)
studyTypeTermKey = 20475461 

# 'Not Resolved' from GXD HT Experiment Type' (vocab key=121)
exptTypeNRKey = 20475438

# 'ArrayExpress' from 'GXD HT Source' (vocab key = 119)
sourceKey = 20475431  

#
# File Descriptors:
#
# ArrayExpress ID file and descriptor
inputDir = os.getenv('INPUTDIR')
inFileName = os.getenv('INPUT_FILE')
fpInFile = None

# QC file and descriptor
qcFileName = os.getenv('QC_RPT')
fpQcFile = None

# Experiment parsing reports
expParsingFileName = os.environ['EXP_PARSING_RPT']
fpExpParsingFile = None

#
# BCP files
#
experimentFileName = os.getenv('EXPERIMENT_BCP')
fpExperimentBcp = None

accFileName = os.getenv('ACC_BCP')
fpAccBcp = None

variableFileName = os.getenv('VARIABLE_BCP')
fpVariableBcp =  None

propertyFileName = os.getenv('PROPERTY_BCP')
fpPropertyBcp = None

# for MGI_Property:
expTypePropKey = 20475425
expFactorPropKey = 20475423
sampleCountPropKey = 20475424
contactNamePropKey = 20475426
namePropKey = 20475428
pubmedPropKey =	20475430 
propTypeKey = 1002

# Number of experiments in AE json file
expCount = 0

# Number experiments loaded
loadedCount = 0

#Number of experiments already in the database
inDbCount = 0

# Number of experiments in the db whose pubmed IDs were updated
updateExptCount = 0

# cache of IDs and counts in the input
# idDict = {primaryID:count, ...}
idDict = {}

# list of files found to be missing
missingFileList = []

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
# {primaryID:key, ...}
primaryIdDict = {}

# raw experiment types mapped to controlled vocabulary keys
exptTypeTransDict = {}

# pubmed properties by AE ID (primary) in the db
# {AE ID: [pubmedId1, ..., pubmedIdn], ...}
pubMedByExptDict = {}

#
# Purpose:  Open file descriptors, get next primary keys, create lookups
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: Copies & opens files, read a database
# Throws: Nothing
#
def initialize():
    global expIdList, fpExpParsingFile
    global fpInFile, fpQcFile, fpExperimentBcp, fpAccBcp, fpVariableBcp 
    global fpPropertyBcp, nextExptKey, nextAccKey, nextExptVarKey
    global nextPropKey

    # create file descriptors
    try:
        fpInFile = open(inFileName, 'r')
    except:
        print('%s does not exist' % inFileName)
    for id in fpInFile.readlines():
        id = str.strip(id)
        if id == '' or id == '#':
            continue
        expIdList.append(id)

    try:
        fpQcFile = open(qcFileName, 'w')
    except:
         print('Cannot create %s' % qcFileName)

    try:
        fpExpParsingFile = open(expParsingFileName, 'w')
    except:
         print('Cannot create %s' % expParsingFileName)


    try:
        fpExperimentBcp = open(experimentFileName, 'w')
    except:
        print('Cannot create %s' % experimentFileName)

    try:
        fpAccBcp = open(accFileName, 'w')
    except:
        print('Cannot create %s' % accFileName)

    try:
        fpVariableBcp = open(variableFileName, 'w')
    except:
        print('Cannot create %s' % variableFileName) 

    try:
        fpPropertyBcp = open(propertyFileName, 'w')
    except:
        print('Cannot create %s' % propertyFileName)

    db.useOneConnection(1)

    # get next primary key for the Experiment table    
    results = db.sql('''select max(_Experiment_key) + 1 as maxKey 
        from GXD_HTExperiment''', 'auto')
    if results[0]['maxKey'] == None:
        nextExptKey = 1000
    else:
        nextExptKey  = results[0]['maxKey']

    # get next primary key for the Accession table
    results = db.sql('''select max(_Accession_key) + 1 as maxKey 
        from ACC_Accession''', 'auto')
    nextAccKey  = results[0]['maxKey']

    # get next primary key for the ExperimentVariable table
    results = db.sql('''select max(_ExperimentVariable_key) + 1 as maxKey 
        from GXD_HTExperimentVariable''', 'auto')
    if results[0]['maxKey'] == None:
        nextExptVarKey = 1000
    else:
        nextExptVarKey  = results[0]['maxKey']

    # get next primary key for the Property table
    results = db.sql('''select max(_Property_key) + 1 as maxKey 
        from MGI_Property''', 'auto')
    if results[0]['maxKey'] == None:
        nextPropKey = 1000
    else:
        nextPropKey  = results[0]['maxKey']

    # Create experiment ID lookup
    results = db.sql('''select accid, _Object_key
        from ACC_Accession
        where _MGIType_key = 42
        and _LogicalDB_key = 189
        and preferred = 1''', 'auto')
    for r in results:
        primaryIdDict[r['accid']] = r['_Object_key']

    # Create experiment type translation lookup
    results = db.sql('''select badname, _Object_key
        from MGI_Translation
        where _TranslationType_key = 1020''', 'auto')
    for r in results:
        exptTypeTransDict[r['badname']] = r['_Object_key']

    # create the pubmed ID property lookup by experiment
    results = db.sql('''select a.accid, p.value
        from GXD_HTExperiment e, ACC_Accession a, MGI_Property p
        where e._Experiment_key = a._Object_key
        and a._MGIType_key = %s
        and a._LogicalDB_key = %s
        and a.preferred = 1
        and e._Experiment_key = p._Object_key
        and p._PropertyTerm_key = %s
        and p._PropertyType_key = %s''' % \
            (mgiTypeKey, aeLdbKey, pubmedPropKey, propTypeKey), 'auto')

    for r in results:
        accid = r['accid'] 
        value = r['value']
        if accid not in pubMedByExptDict:
            pubMedByExptDict[accid] = []
        pubMedByExptDict[accid].append(value)

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
    if type(rawText) == int:
        return 1
    elif type(rawText) == bytes:
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
    
    if ymdMatch:
        (year, month, day) = ymdMatch.groups()
        if (1950 <= int(year) <= 2050):
            if (1 <= int(month) <= 12):
                if (1 <= int(day) <= 31):
                    return 1
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
    if sampleCount and checkInteger(sampleCount)== 0:
        invalidSampleCountDict[primaryID] = sampleCount
        hasError = 1
    # check dates
    if releasedate != '' and checkDate(releasedate) == 0:
        invalidReleaseDateDict[primaryID] = releasedate
        hasError = 1

    if lastupdatedate != '' and checkDate(lastupdatedate) == 0:
        invalidUpdateDateDict[primaryID]= lastupdatedate
        hasError = 1

    return hasError

def processAll():
    for id in expIdList:
        currentExpFile = '%s/%s.json' % (inputDir, id)
        currentSampFile = '%s/%s.sdrf.txt' % (inputDir, id)
        print('currentExpFile: %s' % currentExpFile)
        print('currentSampFile:%s' % currentSampFile)
        try:
            fpExpCurrent = open(currentExpFile, 'r')
        except:
            print('Cannot open %s skipping experiment %s' % (currentExpFile, id)) 
            missingFileList.append(currentExpFile)
            continue
        try:
            fpSampCurrent = open(currentSampFile, 'r')
        except:
            print('Cannot open %s skipping experiment %s' % (currentSampFile, id))
            missingFileList.append(currentSampFile)
            continue
        expJFile = json.load(fpExpCurrent)
        process(expJFile, fpSampCurrent)

def processExperiment(expJFile):
    #print('\nprocessExperiment')
    #
    # Process Experiment
    #
    id = ''
    title = ''
    relDate = ''
    expType = ''
    description = ''
    sampleNum = ''
    authorList = []
    pubMedIdList = []

    id = expJFile['accno']
   
    attr = expJFile['attributes']
    #print('attr: %s\n' % attr)

    for a in attr:
        #print('a: %s' % a)
        if a['name'] == 'Title':
            title = a['value']
        if a['name'] == 'ReleaseDate':
            relDate = a['value']

    section = expJFile['section']
    #print('section: %s' % section)

    sAttr = section['attributes']
    #print('sAttr: %s\n' % sAttr)
    for a in sAttr:
        if a['name'] == 'Study type':
            expType = a['value']
        elif a['name'] == 'Description':
            description  = a['value']

    subsections = section['subsections']
    
    for subs in subsections:
        if type(subs) != list:
            # Note: This is experiment authors NOT publication authors
            if subs['type'] == 'Author':
                authAttr = subs['attributes']
                for author in authAttr:
                    if author['name'] == 'Name':
                        authorList.append(author['value'])
            elif subs['type'] == 'Publication':
                try:
                    # pubmed is only available if status is 'published' i.e.
                    # not there if status is 'submitted' or 'preprint'
                    if subs['accno'] : 
                        pubMedIdList.append(subs['accno'])
                except:
                    print('No PubMed ID')
            elif subs['type'] == 'Samples':
                sampleAttr = subs['attributes']
                for t in sampleAttr:
                    if t['name'] == 'Sample count':
                        sampleNum = t['value']
    fpExpParsingFile.write('%sReport Experiment Attributes%s' % (CRT, CRT))
    fpExpParsingFile.write('id: %s%s' % (id, CRT))
    fpExpParsingFile.write('title :%s%s' % (title, CRT))
    fpExpParsingFile.write('relDate: %s%s' % (relDate, CRT))
    fpExpParsingFile.write('expType: %s%s' % (expType, CRT))
    fpExpParsingFile.write('description: %s%s' % (description, CRT))
    fpExpParsingFile.write('sampleNum: %s%s' % (sampleNum, CRT))
    fpExpParsingFile.write('authors: %s%s' % (authorList, CRT))
    fpExpParsingFile.write('PubMedIds: %s%s' % (pubMedIdList, CRT))

    return id

def processSample(fpSampFile, expID): # later change expID to exptKey

    #print('\nprocessSample')
    headers = str.strip(fpSampFile.readline()) # remove the newline
    tokens = str.split(headers, TAB)

    allHeaderDict = {}
    unitCharHeaderDict = {}

    for t in tokens:
        if str.find(t, 'Unit') == 0:
            unitCharHeaderDict[t] = tokens.index(t)
        elif str.find(t, 'Characteristics') == 0:
            unitCharHeaderDict[t] = tokens.index(t)
        else:
            allHeaderDict[t] = tokens.index(t)
    #print(unitCharHeaderDict)
    #print('\nSample File Body:')
    for line in fpSampFile.readlines():
        line = line[:-1] # remove the newline, don't trim whitespace as last column may be empty
        tokens = str.split(line, TAB)
        #print(tokens) 
        fpExpParsingFile.write('%sReport Sample Named Attributes:%s' % (CRT, CRT))   
        try:
            index = allHeaderDict['Source Name'] 
            source_name = tokens[index]
            fpExpParsingFile.write('source_name: "%s" index: %s%s' % (source_name, index, CRT))
        except:
            source_name = '' 
            fpExpParsingFile.write('missing source_name%s'% CRT)
        try:
            index = allHeaderDict['Comment[ENA_SAMPLE]']       
            ena_sample = tokens[index]
            fpExpParsingFile.write('ena_sample: "%s" index: %s%s' % (ena_sample, index, CRT))
        except:
            ena_sample = ''
            fpExpParsingFile.write('missing ena_sample%s' % CRT)
        try:
            index = allHeaderDict['Comment[BioSD_SAMPLE']
            biosd_sample = tokens[index]
            fpExpParsingFile.write('biosd_sample: "%s" index: %s%s' % (biosd_sample, index, CRT))
        except:
            biosd_sample = ''
            fpExpParsingFile.write('missing biosd_sample%s' % CRT)
        try: 
            index = allHeaderDict['Extract Name']
            extract_name = tokens[index]
            fpExpParsingFile.write('extract_name: "%s" index: %s%s' % (extract_name, index, CRT))
        except:
            extract_name = ''
            fpExpParsingFile.write('missing extract_name%s' % CRT)

        if ena_sample != '':
            sampleID = ena_sample
        elif biosd_sample != '':
            sampleID = biosd_sample
        else:
            sampleID = source_name

        fpExpParsingFile.write('sampleID chosen: %s%s' % (sampleID, CRT))
        fpExpParsingFile.write('%sReport Sample Unit and Characteristic Attributes:%s' % (CRT, CRT))
        for attr in unitCharHeaderDict:
            index = unitCharHeaderDict[attr]
            fpExpParsingFile.write('%s: "%s" index: %s%s' % (attr, tokens[index], index, CRT)) 
    return
#
# Purpose: parse input file, QC, create bcp files
# Returns: 1 if file can be read/processed correctly, else 0
# Assumes: Nothing
# Effects: Creates files in the file system
# Throws: Nothing
#
def process(expJFile, fpSampFile):

    expID = processExperiment(expJFile) # later change return to expKey
    processSample(fpSampFile, expID)

    return

def writeQC():
    fpQcFile.write('GXD HT Raw Data Load QC%s%s%s' % (CRT, CRT, CRT))

    fpQcFile.write('Number of experiments in the input: %s%s' % \
        (expCount, CRT))
    fpQcFile.write('Number of experiments already in the database: %s%s' %\
         (inDbCount, CRT))
    fpQcFile.write('Number of experiments loaded: %s%s' % \
        (loadedCount, CRT))
    fpQcFile.write('Number of experiments with updated PubMed IDs: %s%s%s'\
         % (updateExptCount, CRT, CRT))

    if len(missingFileList):
        fpQcFile.write('Missing Files, experiments not loaded%s' % CRT)
        fpQcFile.write('--------------------------------------------------%s' % CRT)
        for file in missingFileList:
            fpQcFile.write('%s%s' %  (file, CRT))
        fpQcFile.write('%sTotal: %s%s%s' % (CRT, len(missingFileList), CRT, CRT))

    if len(noIdList):
        fpQcFile.write('Experiments with no Primary ID%s' % CRT)
        fpQcFile.write('--------------------------------------------------%s' % CRT)
        for name in noIdList:
            fpQcFile.write('%s%s' %  (name, CRT))
        fpQcFile.write('%sTotal: %s%s%s' % (CRT, len(noIdList), CRT, CRT))

    multiList = []
    for id in idDict:
        if idDict[id] > 1:
            multiList.append('%s%s%d%s' %  (id, TAB, idDict[id], CRT))
    if len(multiList):
        fpQcFile.write('Multiple Experiments with same AE ID%s' % CRT)
        fpQcFile.write('ID%sCount%s' % (TAB, CRT))
        fpQcFile.write('--------------------------------------------------%s' % CRT)
        for line in multiList:
             fpQcFile.write(line)
        fpQcFile.write('%sTotal: %s%s%s' % (CRT, len(multiList), CRT, CRT))

    if len(invalidSampleCountDict):
        fpQcFile.write('Experiments with Invalid Sample Count%s' % CRT)
        fpQcFile.write('ID%sSample Count%s' % (TAB, CRT))
        fpQcFile.write('--------------------------------------------------%s' % CRT)
        for id in invalidSampleCountDict:
            fpQcFile.write('%s%s%s%s' %  (id, TAB, invalidSampleCountDict[id], CRT))
        fpQcFile.write('\nTotal: %s%s%s' % (len(invlaidSampleCountDict), CRT, CRT))

    if len(invalidReleaseDateDict):
        fpQcFile.write('Experiments with Invalid Release Date%s' % CRT)
        fpQcFile.write('ID%sRelease Date%s' % (TAB, CRT))
        fpQcFile.write('--------------------------------------------------%s' % CRT)
        for id in invalidReleaseDateDict:
            fpQcFile.write('%s%s%s%s' %  (id, TAB, invalidReleaseDateDict[id], CRT))
        fpQcFile.write('\nTotal: %s%s%s' % (len(invalidReleaseDateDict), CRT, CRT))

    if len(invalidUpdateDateDict):
        fpQcFile.write('Experiments with Invalid Update Date%s' % CRT)
        fpQcFile.write('ID%sUpdate Date%s' % (TAB, CRT))
        fpQcFile.write('--------------------------------------------------%s' % CRT)
        for id in invalidUpdateDateDict:
            fpQcFile.write('%s%s%s%s' %  (id, TAB, invalidUpdateDateDict[id], CRT))
        fpQcFile.write('\nTotal: %s%s%s' % (len(invalidUpdateDateDict), CRT, CRT))

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
    fpExpParsingFile.close()
    fpExperimentBcp.close()
    fpAccBcp.close()
    fpVariableBcp.close()
    fpPropertyBcp.close()

    return
#
# main
#

initialize()
processAll()
#writeQC()
closeFiles()
