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
MOUSE = 'Mus musculus'

# mapping of exptID to action from the input file
expIdDict = {}

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
inFileName = os.getenv('INPUT_FILE_DEFAULT')
fpInFile = None

# QC file and descriptor
fpCur = open (os.getenv('LOG_CUR'), 'a')

# Experiment parsing reports
expParsingFileName = os.environ['EXP_PARSING_RPT']
fpExpParsingFile = None

#
# BCP files
#
experimentFileName = os.getenv('EXPERIMENT_FILENAME')
fpExperimentBcp = None

sampleFileName = os.environ['SAMPLE_FILENAME']
fpSampleBcp = None

keyValueFileName = os.environ['KEYVALUE_FILENAME']
fpKeyValueBcp = None

accFileName = os.getenv('ACC_FILENAME')
fpAccBcp = None

variableFileName = os.getenv('VARIABLE_FILENAME')
fpVariableBcp =  None

propertyFileName = os.getenv('PROPERTY_FILENAME')
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
exptCount = 0

# Number experiments loaded
exptLoadedCount = 0

#Number of experiments already in the database
inDbCount = 0

# Number of experiments in the db that had raw samples added
updateExptCount = 0

# list of experiment files found to be missing
missingExptFileList = []
missingSampleFileList = []

# None of the 3 sample attributes we use to determine the sample id exist
noSampleIdList = []

# invalid experiment type
invalidExptTypeDict = {}

# Experiments that are not mouse
nonMouseDict = {}

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

#
# Purpose:  Open file descriptors, get next primary keys, create lookups
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: Copies & opens files, read a database
# Throws: Nothing
#
def initialize():
    global expIdDict, fpExpParsingFile
    global fpInFile, fpExperimentBcp, fpAccBcp, fpVariableBcp 
    global fpPropertyBcp, fpKeyValueBcp, nextExptKey, nextAccKey
    global nextExptVarKey, nextPropKey 

    # create file descriptors
    try:
        fpInFile = open(inFileName, 'r')
    except:
        print('%s does not exist' % inFileName)

    for line in fpInFile.readlines():
        (exptID, action) = list(map(str.strip, str.split(line, TAB)))[:2]
        expIdDict[exptID] = action

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

    try:
        fpKeyValueBcp = open(keyValueFileName, 'w')
    except:
        print('Cannot create %s' % keyValueFileName)

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

    db.useOneConnection(0)
    
    return 0

def closeInputFiles(expt, sample):

    expt.close()
    sample.close()

    return 0

def processAll():
    for id in expIdDict:
        action = expIdDict[id]
        currentExpFile = '%s/%s.json' % (inputDir, id)
        currentSampFile = '%s/%s.sdrf.txt' % (inputDir, id)

        print('id: %s action: %s' % (id, action))
        print('currentExpFile: %s' % currentExpFile)
        print('currentSampFile:%s' % currentSampFile)

        # Open file descriptors for Experiment and Sample 
        try:
            fpExpCurrent = open(currentExpFile, 'r')
        except:
            print('Cannot open %s skipping experiment %s' % (currentExpFile, id)) 
            missingExptFileList.append('%s : Experiment File does not Exist' % id)
            continue

        try:
            fpSampCurrent = open(currentSampFile, 'r')
        except:
            print('Cannot open %s skipping experiment %s' % (currentSampFile, id))
            missingSampleFileList.append('%s : Sample File does not Exist' % id)
            fpExpCurrent.close()
            continue

        # sometimes a file downloads, but it is empty, this try except block
        # will capture the error 
        try:
            expJFile = json.load(fpExpCurrent)
        except:
            print('File is empty %s skipping experiment %s' % (currentExpFile, id))
            missingExptFileList.append('%s : Experiment File is Empty' % id)
            closeInputFiles(fpExpCurrent, fpSampCurrent)            
            continue

        # process the experiment
        exptKey = processExperiment(expJFile) 
        print('exptKey: %s' % exptKey)

        # processExperiment handles logging if there are issues processing 
        # if an experiment key is not returned, move on to the next expt.
        if exptKey == 0:
            closeInputFiles(fpExpCurrent, fpSampCurrent)
            continue

        # process the samples
        rc = processSample(fpSampCurrent, id, exptKey)
        print('processSample rc: %s' % rc)
        if rc:
            print('File is empty %s skipping samples for %s' % (currentSampFile, id))
            missingSampleFileList.append('%s : Sample File is Empty' % id)
            closeInputFiles(fpExpCurrent, fpSampCurrent)
            continue

    closeInputFiles(fpExpCurrent, fpSampCurrent)

    return 0

def processExperiment(expJFile):
    global exptCount, noSampleIdList, exptLoadedCount, nonMouseDict
    global inDbCount
    global nextExptKey, nextAccKey, nextExptVarKey, nextPropKey

    print('\nprocessExperiment')
    #
    # Process Experiment
    #
    exptCount += 1
    exptID = ''
    title = ''
    relDate = ''
    exptType = ''
    exptTypeKey = ''
    description = ''
    organism = ''
    sampleNum = ''
    authorList = []
    pubMedIdList = []

    exptID = expJFile['accno']
    print('processExperiment, exptID from file: %s' % exptID)
    if exptID in primaryIdDict:
        inDbCount += 1

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
            exptType = a['value']
        elif a['name'] == 'Description':
            description  = a['value']
        elif a['name'] == 'Organism':
            organism = a['value']
    if exptType not in exptTypeTransDict:
        invalidExptTypeDict[exptID] = exptType
        return 0
    exptTypeKey = exptTypeTransDict[exptType]

    if organism != MOUSE:
        nonMouseDict[exptID] = organism
        return 0
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
    fpExpParsingFile.write('exptType: %s%s' % (exptType, CRT))
    fpExpParsingFile.write('description: %s%s' % (description, CRT))
    fpExpParsingFile.write('organism:  %s%s' % (organism, CRT))
    fpExpParsingFile.write('sampleNum: %s%s' % (sampleNum, CRT))
    fpExpParsingFile.write('authors: %s%s' % (authorList, CRT))
    fpExpParsingFile.write('PubMedIds: %s%s' % (pubMedIdList, CRT))

    # write to bcp files
    #if just samples (update):
    # updateExptCount += 1
    # if new experiments: 
    nextExptKey += 1
    #exptLoadedCount += 1
    
    return nextExptKey

def processSample(fpSampFile, exptID, exptKey): 

    print('\nprocessSample')
    headerLine = fpSampFile.readline()
       
# START - here I need to check for empty file vs missing file
# maybe get the function working first before worrying about this.
    # process the non positional header into data structures
    headerLine = str.strip(headerLine)
    print('Header: %s' % headerLine)
    if headerLine == '':
        print('Empty file')
        return 1
    tokens = str.split(headerLine, TAB)

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

    # process the body of the file
    for line in fpSampFile.readlines():

        # remove the newline, don't trim whitespace as last column may be empty
        line = line[:-1] 

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

        if ena_sample == '' and biosd_sample == '' and source_name == '':
            noSampleIdList.append(exptID)
            fpExpParsingFile.write('No Sample ID, skipping sample:%s' % CRT)
            continue
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
    return 0

def writeQC():
    fpCur.write(' ArrayExpress HT Data Load QC%s%s' % (CRT, CRT))

    fpCur.write('Number of experiments in the input: %s%s' % \
        (exptCount, CRT))
    fpCur.write('Number of experiments already in the database: %s%s' %\
         (inDbCount, CRT))
    fpCur.write('Number of experiments loaded: %s%s' % \
        (exptLoadedCount, CRT))
    fpCur.write('Number of experiments updated with Raw Samples: %s%s%s'\
         % (updateExptCount, CRT, CRT))

    if len(missingExptFileList):
        fpCur.write('Missing or Empty Experiment Files, experiments not loaded%s' % CRT)
        fpCur.write('--------------------------------------------------%s' % CRT)
        for file in missingExptFileList:
            fpCur.write('%s%s' %  (file, CRT))
        fpCur.write('%sTotal: %s%s%s' % (CRT, len(missingExptFileList), CRT, CRT))

    if len(missingSampleFileList):
        fpCur.write('Missing or Empty Sample Files, experiments not loaded%s' % CRT)
        fpCur.write('--------------------------------------------------%s' % CRT)
        for file in missingSampleFileList:
            fpCur.write('%s%s' %  (file, CRT))
        fpCur.write('%sTotal: %s%s%s' % (CRT, len(missingSampleFileList), CRT, CRT))


    if len(invalidExptTypeDict):
        fpCur.write('Experiments with Invalid Experiment Type,  experiments not loaded%s' % CRT)
        fpCur.write('--------------------------------------------------%s' % CRT)

        for id in invalidExptTypeDict:
            fpCur.write('%s%s%s%s' %  (id, TAB, invalidExptTypeDict[id], CRT))
        fpCur.write('\nTotal: %s%s%s' % (len(invalidExptTypeDict), CRT, CRT))

    if len(nonMouseDict):
        fpCur.write('Non-Mouse Experiments, experiments not loaded%s' % CRT)
        fpCur.write('ID%sOrganism%s' % (TAB, CRT))
        fpCur.write('--------------------------------------------------%s' % CRT)
        for id in nonMouseDict:
            fpCur.write('%s%s%s%s' %  (id, TAB, nonMouseDict[id], CRT))
        fpCur.write('\nTotal: %s%s%s' % (len(nonMouseDict), CRT, CRT))


    if len(invalidSampleCountDict):
        fpCur.write('Experiments with Invalid Sample Count%s' % CRT)
        fpCur.write('ID%sSample Count%s' % (TAB, CRT))
        fpCur.write('--------------------------------------------------%s' % CRT)
        for id in invalidSampleCountDict:
            fpCur.write('%s%s%s%s' %  (id, TAB, invalidSampleCountDict[id], CRT))
        fpCur.write('\nTotal: %s%s%s' % (len(invalidSampleCountDict), CRT, CRT))

    if len(invalidReleaseDateDict):
        fpCur.write('Experiments with Invalid Release Date%s' % CRT)
        fpCur.write('ID%sRelease Date%s' % (TAB, CRT))
        fpCur.write('--------------------------------------------------%s' % CRT)
        for id in invalidReleaseDateDict:
            fpCur.write('%s%s%s%s' %  (id, TAB, invalidReleaseDateDict[id], CRT))
        fpCur.write('\nTotal: %s%s%s' % (len(invalidReleaseDateDict), CRT, CRT))

    if len(invalidUpdateDateDict):
        fpCur.write('Experiments with Invalid Update Date%s' % CRT)
        fpCur.write('ID%sUpdate Date%s' % (TAB, CRT))
        fpCur.write('--------------------------------------------------%s' % CRT)
        for id in invalidUpdateDateDict:
            fpCur.write('%s%s%s%s' %  (id, TAB, invalidUpdateDateDict[id], CRT))
        fpCur.write('\nTotal: %s%s%s' % (len(invalidUpdateDateDict), CRT, CRT))

    if len(noSampleIdList):
        fpCur.write('Samples with no ID, sample not loaded%s' % CRT)
        fpCur.write('--------------------------------------------------%s' % CRT)
        for name in noSampleIdList:
            fpCur.write('%s%s' %  (name, CRT))
        fpCur.write('%sTotal: %s%s%s' % (CRT, len(noSampleIdList), CRT, CRT))


    return 0

#
# Purpose: Close file descriptors
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: Closes files
# Throws: Nothing
#
def closeFiles():

    fpInFile.close()
    fpCur.close()
    fpExpParsingFile.close()
    fpExperimentBcp.close()
    fpAccBcp.close()
    fpVariableBcp.close()
    fpPropertyBcp.close()

    return 0
#
# main
#

initialize()
processAll()
writeQC()
closeFiles()
