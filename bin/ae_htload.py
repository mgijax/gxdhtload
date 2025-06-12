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
import re
import json
import db
import loadlib
import accessionlib

TAB = '\t'
CRT = '\n'
MOUSE = 'mus'

# default experiment confidence value
confidence = 0.0

# mapping of exptID to action from the input file
exptIdDict = {}

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

# null values in gxd_htexperiment
updateDate = ''
evalDate = ''
evalByKey = ''
initCurByKey = ''
lastCurByKey = ''
initCurDate = ''
lastCurDate = ''

# For GXD_HTExperimentVariable:
# 'Not Curated' from 'GXD HT Variables' (vocab key=122)
exptVariableTermKey = 20475439 

# For GXD_HTExperiment:
# 'Not Evaluated' from 'GXD HT Evaluation State' (vocab key = 116) 
defaultEvalStateTermKey = 100079348 

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

# For GXD_HTRawSample
rawSampleMgiTypeKey = 47

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
# For bcp
#

bcpin = '%s/bin/bcpin.csh' % os.environ['PG_DBUTILS']
server = os.environ['MGD_DBSERVER']
database = os.environ['MGD_DBNAME']

expt_table = 'GXD_HTExperiment'
acc_table = 'ACC_Accession'
exptvar_table = 'GXD_HTExperimentVariable'
property_table = 'MGI_Property'
sample_table = 'GXD_HTRawSample'
keyvalue_table = 'MGI_KeyValue'
outputDir = os.environ['OUTPUTDIR']

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

# for MGI_Propertay (experiments)
expTypePropKey = 20475425
expFactorPropKey = 20475423
sampleCountPropKey = 20475424
contactNamePropKey = 20475426
namePropKey = 20475428
pubmedPropKey =	20475430 
propTypeKey = 1002

# For MGI_KeyValue (samples)
# These are controlled column header values from the sample file
# the unit and characteristic keys are not controlled therefor we take
# what is in the file
rawSourceKey = 'Source Name'
rawEnaSampleKey = 'ENA_SAMPLE'
rawBioSdSampleKey = 'BioSD_SAMPLE'
rawExtractNameKey = 'Extract Name'

# Number of experiments in AE json file
exptCount = 0

# Number experiments loaded
exptLoadedCount = 0

# Number of experiments already in the database
inDbCount = 0

# Number of samples loaded for new experiments
newExptSamplesLoadedCount = 0

# Number of experiments in the db that had raw samples added
updateExptCount = 0

# Number of experiments loaded for existing experiments
existingExptSamplesLoadedCount = 0

# list of experiment files found to be missing
missingExptFileList = []
missingSampleFileList = []
emptySampleFileList = []

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

# database lookups

# AE IDs in the database
# {primaryID:key, ...}
primaryIdDict = {}

# raw experiment types mapped to controlled vocabulary keys
exptTypeTransDict = {}

class Experiment:
    # Is: data object that represents an experiment 
    # Has: an experiment ID and it's database key, either existing or new 
    # Does: provides direct access to its attributes
    #
    def __init__ (self):
        # Purpose: constructor
        # Returns: nothing
        # Assumes: nothing
        # Effects: nothing
        # Throws: nothing
        self.id = None
        self.key = None
        self.existing = None
        
# end class Experiment -----------------------------------------


#
# Purpose:  Open file descriptors, get next primary keys, create lookups
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: Copies & opens files, read a database
# Throws: Nothing
#
def initialize():
    global exptIdDict, fpExpParsingFile, exptCount
    global fpInFile, fpExperimentBcp, fpSampleBcp, fpAccBcp, fpVariableBcp 
    global fpPropertyBcp, fpKeyValueBcp, nextExptKey, nextAccKey
    global nextExptVarKey, nextPropKey, nextRawSampleKey, nextKeyValueKey

    # create file descriptors
    try:
        fpInFile = open(inFileName, 'r')
    except:
        print('%s does not exist' % inFileName)

    for line in fpInFile.readlines():
        (exptID, action) = list(map(str.strip, str.split(line, TAB)))[:2]
        exptIdDict[exptID] = action
    exptCount = len(exptIdDict)
    try:
        fpExpParsingFile = open(expParsingFileName, 'w')
    except:
         print('Cannot create %s' % expParsingFileName)

    try:
        fpExperimentBcp = open('%s/%s' % (outputDir, experimentFileName), 'w')
    except:
        print('Cannot create %s' % experimentFileName)

    try:
        fpSampleBcp = open('%s/%s' % (outputDir, sampleFileName), 'w')
    except:
        print('Cannot create %s' % sampleFileName)

    try:
        fpAccBcp = open('%s/%s' % (outputDir, accFileName), 'w')
    except:
        print('Cannot create %s' % accFileName)

    try:
        fpVariableBcp = open('%s/%s' % (outputDir, variableFileName), 'w')
    except:
        print('Cannot create %s' % variableFileName) 

    try:
        fpPropertyBcp = open('%s/%s' % (outputDir, propertyFileName), 'w')
    except:
        print('Cannot create %s' % propertyFileName)

    try:
        fpKeyValueBcp = open('%s/%s' % (outputDir, keyValueFileName), 'w')
    except:
        print('Cannot create %s' % keyValueFileName)

    db.useOneConnection(1)

    # get next primary key for the Accession table
    results = db.sql('''select max(_Accession_key) + 1 as maxKey from ACC_Accession''', 'auto')
    nextAccKey  = results[0]['maxKey']

    # get next primary key for the Experiment table
    results = db.sql(''' select nextval('gxd_htexperiment_seq') as maxKey ''', 'auto')
    nextExptKey  = results[0]['maxKey']

    # get next primary key for the ExperimentVariable table
    results = db.sql(''' select nextval('gxd_htexperimentvariable_seq') as maxKey ''', 'auto')
    nextExptVarKey  = results[0]['maxKey']

    # get next primary key for the Raw Sample table
    results = db.sql(''' select nextval('gxd_htrawsample_seq') as maxKey ''', 'auto')
    nextRawSampleKey = results[0]['maxKey']

    # get next primary key for the Key Value table
    results = db.sql(''' select nextval('mgi_keyvalue_seq') as maxKey ''', 'auto')
    nextKeyValueKey = results[0]['maxKey']

    # get next primary key for the Property table
    results = db.sql(''' select nextval('mgi_property_seq') as maxKey ''', 'auto')
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

# end initialize() -----------------------------------------


#
# Purpose: close input file descriptors only 
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: closes file descriptors
# Throws: Nothing
#

def closeInputFiles(expt, sample):

    expt.close()
    sample.close()

    return 0

# end closeInputFiles() -----------------------------------------

#
# Purpose: The main loop for processing experiments and samples 
# Returns: 1 if runtime error, else 0
# Assumes: Nothing
# Effects: writes to logs, reports and bcp files
# Throws: Nothing
#

def processAll():
    for id in exptIdDict:
        action = exptIdDict[id]
        currentExpFile = '%s/%s.json' % (inputDir, id)
        currentSampFile = '%s/%s.sdrf.txt' % (inputDir, id)

        #print('id: %s action: %s' % (id, action))
        #print('currentExpFile: %s' % currentExpFile)
        #print('currentSampFile:%s' % currentSampFile)

        # Open file descriptors for Experiment and Sample 
        try:
            fpExpCurrent = open(currentExpFile, 'r')
        except:
            missingExptFileList.append('%s : Experiment File does not Exist' % id)
            continue

        try:
            fpSampCurrent = open(currentSampFile, 'r')
        except:
            missingSampleFileList.append('%s : Sample File does not Exist' % id)
            fpExpCurrent.close()
            continue

        # sometimes the input file is empty
        try:
            expJFile = json.load(fpExpCurrent)
        except:
            #print('File is empty %s skipping experiment %s' % (currentExpFile, id))
            missingExptFileList.append(id)
            closeInputFiles(fpExpCurrent, fpSampCurrent)            
            continue

        # sometimes the input file does not contain "accno"
        if 'accno' not in expJFile:
            print('File is missing accno line %s skipping experiment %s' % (currentExpFile, id))
            closeInputFiles(fpExpCurrent, fpSampCurrent)            
            continue

        # process the experiment
        experiment = processExperiment(expJFile)

        # processExperiment handles logging if there are issues processing 
        # if an experiment key is not returned, move on to the next expt.
        if experiment.key == 0:
            closeInputFiles(fpExpCurrent, fpSampCurrent)
            continue

        print('exptID: %s exptKey: %s exptExisting: %s' % ( experiment.id, experiment.key, experiment.existing))
        # process the samples
        rc = processSample(fpSampCurrent, experiment)
        print('processSample rc: %s' % rc)
        if rc:
            #print('File is empty %s skipping samples for %s' % (currentSampFile, id))
            emptySampleFileList.append(id)
            closeInputFiles(fpExpCurrent, fpSampCurrent)
            continue

    return 0

# end processAll() -----------------------------------------

#
# Purpose: Processes an experiment
# Returns: And Experiment object
# Assumes: Nothing
# Effects: Writes to logs, reports and bcp files
# Throws: Nothing
#

def processExperiment(expJFile):
    global noSampleIdList, exptLoadedCount, nonMouseDict
    global inDbCount, exptUpdateCount
    global nextExptKey, nextAccKey, nextExptVarKey, nextPropKey

    currentExptKey = nextExptKey

    #
    # Process Experiment
    #
    exptID = ''
    title = ''
    relDate = ''
    exptType = ''
    exptTypeKey = ''
    description = ''
    organism = ''
    sampleCount = ''
    authorList = []
    pubMedIdList = []

    exptID = expJFile['accno']
    print('processExperiment, exptID from file: %s' % exptID)
    fpExpParsingFile.write('%sProcessing "%s"%s' % (CRT, exptID, CRT))
    experiment = Experiment()
    experiment.id = exptID

    # if the experiment is in the database return the database key
    if exptID in primaryIdDict:
        inDbCount += 1
        experiment.key = primaryIdDict[exptID]
        experiment.existing = 1
        return experiment

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
    exptTypeList = []
    organismList = []
    for a in sAttr:
        if a['name'] == 'Study type':
            exptTypeList.append(a['value'])
        elif a['name'] == 'Description':
            description  = a['value']
        elif a['name'] == 'Organism':
            organismList.append(str.lower(a['value']))
    for eType in exptTypeList:
        if eType in exptTypeTransDict:
            exptType = eType
            break
    if exptType == '':
            invalidExptTypeDict[exptID] = exptTypeList
            experiment.key = 0
            return experiment
    exptTypeKey = exptTypeTransDict[exptType]

    #print('organismList: %s' % organismList)
    isMouse = 0
    for org in organismList:
        #print('org: %s' % org)
        #if org == MOUSE:
        if str.find(org, MOUSE) == 0:
            #print('organism is Mus, load')
            #organism == org
            isMouse = 1
            break
    #if organism != MOUSE:
    if not isMouse:
        #print('organism is not Mus, skip: %s' % organism)
        nonMouseDict[exptID] = organismList
        experiment.key = 0
        return experiment
    subsections = section['subsections']
    #print('subsections: %s\n\n' % subsections)
    for subs in subsections:
        #print('subs: %s\n' % subs)
        if type(subs) != list and subs != {}:
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
                        sampleCount = t['value']
    fpExpParsingFile.write('%sReport Experiment Attributes for "%s"%s' % (CRT, exptID, CRT))
    fpExpParsingFile.write('title :%s%s' % (title, CRT))
    fpExpParsingFile.write('relDate: %s%s' % (relDate, CRT))
    fpExpParsingFile.write('exptType: %s%s' % (exptTypeList, CRT))
    fpExpParsingFile.write('description: %s%s' % (description, CRT))
    fpExpParsingFile.write('organism:  %s%s' % (organismList, CRT))
    fpExpParsingFile.write('sampleCount: %s%s' % (sampleCount, CRT))
    fpExpParsingFile.write('authors: %s%s' % (authorList, CRT))
    fpExpParsingFile.write('PubMedIds: %s%s' % (pubMedIdList, CRT))

    #
    # GXD_HTExperiment BCP
    #

    line = '%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (currentExptKey, TAB, sourceKey, TAB, title, TAB, description, TAB, relDate, TAB, updateDate, TAB, evalDate, TAB, defaultEvalStateTermKey, TAB, curStateTermKey, TAB, studyTypeTermKey, TAB, exptTypeKey, TAB, evalByKey, TAB, initCurByKey, TAB, lastCurByKey, TAB, initCurDate, TAB, lastCurDate, TAB, confidence, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT)
    #print('line: %s' % line)
    fpExperimentBcp.write(line)

    #
    # GXD_HTVariable BCP
    #
    fpVariableBcp.write('%s%s%s%s%s%s' % (nextExptVarKey, TAB, currentExptKey, TAB, exptVariableTermKey, CRT))
    nextExptVarKey += 1

    #
    # ACC_Accession BCP
    #
    prefixPart, numericPart = accessionlib.split_accnum(exptID)
    fpAccBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextAccKey, TAB, exptID, TAB, prefixPart, TAB, numericPart, TAB, aeLdbKey, TAB, currentExptKey, TAB, mgiTypeKey, TAB, private, TAB, isPreferred, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT ))
    nextAccKey += 1

    #
    # Experiment Properties
    #

    # title (1) experiment name
    #   
    # sampleCount(1) count of samples
    #   
    # exptTypeList (1-n) raw experiment types
    #   
    # pubMedIdList (0-n) pubmed Ids
    #   
    # description (1) sample overalldesign + expt summary
    #   
    # the template for properties:
    propertyTemplate = "#====#%s%s%s#=#%s%s%s%s%s#==#%s#===#%s%s%s%s%s%s%s%s%s" % (TAB, propTypeKey, TAB, TAB, currentExptKey, TAB, mgiTypeKey, TAB, TAB, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT )

    if title != '':
        toLoad = propertyTemplate.replace('#=#', str(namePropKey)).replace('#==#', title).replace('#===#', '1').replace('#====#', str(nextPropKey))
        fpPropertyBcp.write(toLoad)
        nextPropKey += 1

    if sampleCount != '':
        toLoad = propertyTemplate.replace('#=#', str(sampleCountPropKey)).replace('#==#', sampleCount).replace('#===#', '1').replace('#====#', str(nextPropKey))
        fpPropertyBcp.write(toLoad)
        nextPropKey += 1

    seqNumCt = 1
    for e in exptTypeList:
        toLoad = propertyTemplate.replace('#=#', str(expTypePropKey)).replace('#==#', e).replace('#===#', str(seqNumCt)).replace('#====#', str(nextPropKey))
        fpPropertyBcp.write(toLoad)
        seqNumCt += 1
        nextPropKey += 1

    for b in pubMedIdList:
        toLoad = propertyTemplate.replace('#=#', str(pubmedPropKey)).replace('#==#', str(b)).replace('#===#', str(seqNumCt)).replace('#====#', str(nextPropKey))
        fpPropertyBcp.write(toLoad)
        seqNumCt += 1
        nextPropKey += 1

    nextExptKey += 1
    exptLoadedCount += 1

    # New experiment, return new experiment key
    experiment.key = currentExptKey
    experiment.existing = 0

    return experiment

# end processExperiment() -----------------------------------------

#
# Purpose: Processes all the samples for an experiment
# Returns: 1 if there is no header line or if runtime error 
# Assumes: Nothing
# Effects: Writes to logs, reports and bcp files
# Throws: Nothing
#

def processSample(fpSampFile, experiment): 
    global nextRawSampleKey, nextKeyValueKey, updateExptCount
    global newExptSamplesLoadedCount, existingExptSamplesLoadedCount

    # unpack our experiment
    exptID = experiment.id
    exptKey = experiment.key
    existing = experiment.existing

    print('processSample, exptID: %s' % exptID)
    headerLine = fpSampFile.readline()
       
    # process the non positional header into data structures
    headerLine = str.strip(headerLine)
    
    if headerLine == '':
        return 1

    tokens = str.split(headerLine, TAB)

    allHeaderDict = {}
    unitCharFactorHeaderDict = {}

    for t in tokens:
        if str.find(t, 'Unit') == 0:
            
            unitCharFactorHeaderDict[t] = tokens.index(t)
        elif str.find(t, 'Characteristics') == 0:
            unitCharFactorHeaderDict[t] = tokens.index(t)
        elif str.find(t, 'Factor Value') == 0:
            unitCharFactorHeaderDict[t] = tokens.index(t)
        else:
            allHeaderDict[t] = tokens.index(t)
    # We need to collapse the sample attributes we want to load by the
    # sampleID that was chosen, otherwise we will load duplicates.
    sampleDict = {}

    # process the body of the file
    for line in fpSampFile.readlines():

        # remove the newline, don't trim whitespace as last column may be empty
        line = line[:-1] 
        
        source_name = ''
        ena_sample = ''
        biosd_sample = ''
        extract_name = ''
        tokens = str.split(line, TAB)

        try:
            index = allHeaderDict['Source Name'] 
            source_name = tokens[index]
        except:
            fpExpParsingFile.write('missing source_name%s'% CRT)
        try:
            index = allHeaderDict['Comment[ENA_SAMPLE]']       
            ena_sample = tokens[index]
        except:
            fpExpParsingFile.write('missing ena_sample%s' % CRT)
        try: 
            index = allHeaderDict['Comment[BioSD_SAMPLE]']
            biosd_sample = tokens[index]
        except:
            fpExpParsingFile.write('missing biosd_sample%s' % CRT)
        try: 
            index = allHeaderDict['Extract Name']
            extract_name = tokens[index]
        except:
            fpExpParsingFile.write('missing extract_name%s' % CRT)

        if ena_sample == '' and biosd_sample == '' and source_name == '':
            noSampleIdList.append(exptID)
            continue
        if ena_sample != '':
            sampleID = ena_sample
        elif biosd_sample != '':
            sampleID = biosd_sample
        else:
            sampleID = source_name
        
        # Mapping of controlled sample attributes (key) to their values by variable name
        # Some of these may be empty string
        #
        # rawSourceKey = 'Source Name' --> value = source_name
        # rawEnaSampleKey = 'ENA_SAMPLE' --> value = ena_sample
        # rawBioSdSampleKey = 'BioSD_SAMPLE' --> value = biosd_sample
        # rawExtractNameKey = 'Extract Name' --> value = extract_name

        attrList = []
        attrList.append(source_name)
        attrList.append(ena_sample)
        attrList.append(biosd_sample)
        attrList.append(extract_name)
       
        # now add the unit, characteristics and factor key/values 
        attrRE = re.compile(r"\[(.+)\]")
        for attr in unitCharFactorHeaderDict:
            r = attrRE.search(attr)
            key = r.group(1)
            index = unitCharFactorHeaderDict[attr]
            try:
                value = str.strip(tokens[index])
            except:
                value = ''
            if value != None and value != '':
                attrList.append('%s|%s' % (key, value))
  
        # map the attrList to it's sample id, there may be multiple? 
        if sampleID not in sampleDict:
            sampleDict[sampleID] = []
        sampleDict[sampleID].append(attrList)
    #
    # Now write to report and bcp
    #
    for sampleID in sampleDict:
        # get the first list of sample attributes for this sample, some samples have 
        # multiple attributes (which we do not load) which means there are duplicate lines
        samplesToWriteList = sampleDict[sampleID][0]

        # get the named attributes
        namedSampleAttrList = samplesToWriteList[0:4]

        # get the unit and characteristic attributes
        unitCharFactorAttrList = samplesToWriteList[4:]

        # write to fpSampleBcp 
        fpSampleBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextRawSampleKey, TAB, exptKey, TAB, sampleID, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT))
        if existing == 0:
            newExptSamplesLoadedCount += 1
        else:
            existingExptSamplesLoadedCount += 1

        #
        # write Named attributes to parsing report and to bcp, if any are empty string
        # skip it
        #
        fpExpParsingFile.write('%sReport Sample Attributes for "%s"/"%s"%s' % (CRT, exptID, sampleID, CRT))
        source_name = namedSampleAttrList[0]
        ena_sample = namedSampleAttrList[1]
        biosd_sample = namedSampleAttrList[2]
        extract_name = namedSampleAttrList[3]

        
        # write to parsing report and bcp
        if source_name:
            fpExpParsingFile.write('source_name: "%s"%s' % (source_name, CRT))
            fpKeyValueBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextKeyValueKey, TAB, nextRawSampleKey, TAB, rawSampleMgiTypeKey, TAB, rawSourceKey, TAB, source_name, TAB, '1', TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT))
            nextKeyValueKey += 1

        if ena_sample:
            fpExpParsingFile.write('ena_sample: "%s"%s' % (ena_sample, CRT))
            fpKeyValueBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextKeyValueKey, TAB, nextRawSampleKey, TAB, rawSampleMgiTypeKey, TAB, rawEnaSampleKey, TAB, ena_sample, TAB, '1', TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT))
            nextKeyValueKey += 1

        if biosd_sample:
            fpExpParsingFile.write('biosd_sample: "%s"%s' % (biosd_sample, CRT))
            fpKeyValueBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextKeyValueKey, TAB, nextRawSampleKey, TAB, rawSampleMgiTypeKey, TAB, rawBioSdSampleKey, TAB, biosd_sample, TAB, '1', TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT))
            nextKeyValueKey += 1

        if extract_name:
            fpExpParsingFile.write('extract_name: "%s"%s' % (extract_name, CRT))
            fpKeyValueBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextKeyValueKey, TAB, nextRawSampleKey, TAB, rawSampleMgiTypeKey, TAB, rawExtractNameKey, TAB, extract_name, TAB, '1', TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT))
            nextKeyValueKey += 1

        #
        # write Unit, Characteristics and Factor attributes to parsing report i
        # and to bcp
        #
        for uca in unitCharFactorAttrList:
            (key, value) = str.split(uca, '|')
            fpExpParsingFile.write('%s: "%s"%s' % (key, value, CRT))
            fpKeyValueBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextKeyValueKey, TAB, nextRawSampleKey, TAB, rawSampleMgiTypeKey, TAB, key, TAB, value, TAB, '1', TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT))
            nextKeyValueKey += 1
                
        nextRawSampleKey += 1

    # If the expeiment exists in the datase w/o samples, samples will be added
    # which is an experiment update
    if experiment.existing:
        updateExptCount += 1
    return 0

# end processSample() -----------------------------------------

#
# Purpose: Writes load QC to the curator log
# Returns: non-zero if runtime error, else 0
# Assumes: Nothing
# Effects: Writes to the curator log
# Throws: Nothing
#

def writeQC():
    fpCur.write('ArrayExpress HT Data Load QC%s%s' % (CRT, CRT))

    fpCur.write('Number of experiments in the input file: %s%s' % \
        (exptCount, CRT))
    fpCur.write('Number of experiments already in the database: %s%s' %\
         (inDbCount, CRT))
    fpCur.write('Number of new experiments loaded: %s%s' % \
        (exptLoadedCount, CRT))
    fpCur.write('Number of raw samples loaded for new experiments: %s%s' % \
        (newExptSamplesLoadedCount, CRT))
    fpCur.write('Number of existing experiments updated with raw samples: %s%s'\
         % (updateExptCount, CRT))
    fpCur.write('Number of raw samples loaded for existing experiments: %s%s%s' % \
        (existingExptSamplesLoadedCount, CRT, CRT))

    if len(missingExptFileList):
        fpCur.write('Missing or Empty Experiment Files, experiments not loaded%s' % CRT)
        fpCur.write('--------------------------------------------------%s' % CRT)
        for file in missingExptFileList:
            fpCur.write('%s%s' %  (file, CRT))
        fpCur.write('%sTotal: %s%s%s' % (CRT, len(missingExptFileList), CRT, CRT))

    if len(missingSampleFileList):
        fpCur.write('Missing Sample Files. If add - experiments not loaded, if update - samples not loaded%s' % CRT)
        fpCur.write('--------------------------------------------------%s' % CRT)
        for file in missingSampleFileList:
            fpCur.write('%s%s' %  (file, CRT))
        fpCur.write('%sTotal: %s%s%s' % (CRT, len(missingSampleFileList), CRT, CRT))

    if len(emptySampleFileList):
        fpCur.write('Empty Sample Files. If add - experiments not loaded, if update - samples not loaded%s' % CRT)
        fpCur.write('--------------------------------------------------%s' % CRT)
        for file in emptySampleFileList:
            fpCur.write('%s%s' %  (file, CRT))
        fpCur.write('%sTotal: %s%s%s' % (CRT, len(emptySampleFileList), CRT, CRT))

    if len(invalidExptTypeDict):
        fpCur.write('Experiments with No Valid Experiment Types,  experiments not loaded%s' % CRT)
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

    if len(noSampleIdList):
        fpCur.write('Samples with no ID, sample not loaded%s' % CRT)
        fpCur.write('--------------------------------------------------%s' % CRT)
        for name in noSampleIdList:
            fpCur.write('%s%s' %  (name, CRT))
        fpCur.write('%sTotal: %s%s%s' % (CRT, len(noSampleIdList), CRT, CRT))


    return 0

# end writeQC() -----------------------------------------

#
# Purpose: executes bcp
# Returns: non-zero if bcp error, else 0
# Assumes:
# Effects: executes bcp, writes to the database
# Throws: Nothing
#

def doBCP():

    rc = 0

    if os.path.getsize(outputDir + "/" + experimentFileName) > 0:
        bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, expt_table, outputDir, experimentFileName)
        print('bcpCmd: %s' % bcpCmd)
        rc = os.system(bcpCmd)
        if rc:
            return rc

    if os.path.getsize(outputDir + "/" + accFileName) > 0:
        bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, acc_table, outputDir, accFileName)
        print('bcpCmd: %s' % bcpCmd)
        rc = os.system(bcpCmd)
        if rc:
            return rc

    if os.path.getsize(outputDir + "/" + variableFileName) > 0:
        bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, exptvar_table, outputDir, variableFileName)
        print('bcpCmd: %s' % bcpCmd)
        rc = os.system(bcpCmd)
        if rc:
            return rc

    if os.path.getsize(outputDir + "/" + propertyFileName) > 0:
        bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, property_table, outputDir, propertyFileName)
        print('bcpCmd: %s' % bcpCmd)
        rc = os.system(bcpCmd)
        if rc:
            return rc

    if os.path.getsize(outputDir + "/" + sampleFileName) > 0:
        bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, sample_table, outputDir, sampleFileName)
        print('bcpCmd: %s' % bcpCmd)
        rc = os.system(bcpCmd)
        if rc:
            return rc

    if os.path.getsize(outputDir + "/" + keyValueFileName) > 0:
        bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, keyvalue_table, outputDir, keyValueFileName)
        print('bcpCmd: %s' % bcpCmd)
        rc = os.system(bcpCmd)
        if rc:
            return rc

    # update gxd_htexperiment_seq auto-sequence
    db.sql(''' select setval('gxd_htexperiment_seq', (select max(_Experiment_key) from GXD_HTExperiment)) ''', None)

    # update gxd_htexperimentvariable_seq auto-sequence
    db.sql(''' select setval('gxd_htexperimentvariable_seq', (select max(_ExperimentVariable_key) from GXD_HTExperimentVariable)) ''', None)

    # update gxd_htrawsample_seq auto-sequence
    db.sql(''' select setval('gxd_htrawsample_seq', (select max(_RawSample_key) from GXD_HTRawSample)) ''', None)

    # update mgi_keyvalue_seq auto-sequence
    db.sql(''' select setval('mgi_keyvalue_seq', (select max(_KeyValue_key) from MGI_KeyValue)) ''', None)

    # update mgi_property_seq auto-sequence
    db.sql(''' select setval('mgi_property_seq', (select max(_Property_key) from MGI_Property)) ''', None)

    return rc

# end doBCP() -----------------------------------------

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
    fpSampleBcp.close()
    fpKeyValueBcp.close()
    return 0

# end closeFiles() -----------------------------------------

#
# main
#
if initialize() != 0:
    print("ae_htload failed initializing")
    sys.exit(1)

if processAll() != 0:
    print("ae_htload failed during processing")
    closeFiles()
    sys.exit(1)

if writeQC() != 0:
    print("ae_htload failed writing QC")
    closeFiles()
    sys.exit(1)

if closeFiles() != 0:
    print("ae_htload failed closing files")
    sys.exit(1)

if doBCP() != 0:
   print("ae_htload failed doing BCP")
   sys.exit(1)
