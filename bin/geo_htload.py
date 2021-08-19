'''
#
# geo_htload.py
#
#	See http://mgiwiki/mediawiki/index.php/sw:Gxdhtload
#
# Usage:
#       geo_htload.py
#
# History:
#
# sc   06/17/2010
#       - created WTS2-431
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
import xml.etree.ElementTree as ET

TAB = '\t'
CRT = '\n'

# load only experiments with <= MAX_SAMPLES
maxSamples = os.environ['MAX_SAMPLES']

# string that identifies supereries which we will not load
SUPERSERIES='This SuperSeries'

# today's date
loadDate = loadlib.loaddate

# evaluation date - null
evalDate = ''

# user creating the database records, gxdhtload
# used for record createdBy and modifiedBy
userKey = 1561 

#
# For ACC_Accession:
#
# Experiment MGIType key
exptMgiTypeKey = 42

# GEO LogicalDB key
geoLdbKey = 190

isPreferred = 1
notPreferred = 0
private = 0

# For GXD_HTExperimentVariable:
# 'Not Curated' from 'GXD HT Variables' (vocab key=122)
exptVariableTermKey = 20475439 

# For GXD_HTExperiment:
# 'Not Evaluated' from 'GXD HT Evaluation State' (vocab key = 116) 
evalStateTermKey = 20225941 

# 'Not Done' from GXD HT Curation State' (vocab key=117)
curStateTermKey = 20475422

# 'Not Curated' from 'GXD HT Study Type' (vocab key=124)
studyTypeTermKey = 20475461 

# null values in gxd_htexperiment
evalByKey = ''
initCurByKey = ''
lastCurByKey = ''
initCurDate = ''
lastCurDate = ''
releasedate = ''

# 'GEO' from 'GXD HT Source' (vocab key = 119)
sourceKey = 87145238

# For GXD_HTRawSample
rawSampleMgiTypeKey = 47

#
# File Descriptors:
#

# to form the sample file to process
geoDownloads = os.environ['GEO_DOWNLOADS']
sampleFileSuffix = os.environ['GEO_SAMPLE_FILE_SUFFIX']

# QC file and descriptor
qcFileName = os.environ['QC_RPT']
fpQcFile = None

# Experiment parsing reports
expParsingFileName = os.environ['EXP_PARSING_RPT']
fpExpParsingFile = None

# Sample parsing report
sampParsingFileName  = os.environ['SAMP_PARSING_RPT']
fpSampParsingFile = None

# run parsing reports true/false
runParsingReports = os.environ['RUN_PARSING_RPTS']

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

experimentFileName = os.environ['EXPERIMENT_FILENAME']
fpExperimentBcp = None

sampleFileName = os.environ['SAMPLE_FILENAME']
fpSampleBcp = None

keyValueFileName = os.environ['KEYVALUE_FILENAME']
fpKeyValueBcp = None

accFileName = os.environ['ACC_FILENAME']
fpAccBcp = None

variableFileName = os.environ['VARIABLE_FILENAME']
fpVariableBcp =  None

propertyFileName = os.environ['PROPERTY_FILENAME']
fpPropertyBcp = None

# for MGI_Property

# GXD HT Experiment Property vocab
expTypePropKey = 20475425       # raw experiment type
sampleCountPropKey = 20475424   # raw sample count
namePropKey = 20475428          # raw name
pubmedPropKey =	20475430        # PubMed ID
#descriptionPropKey = 87508020   # raw description DO WE NEED THIS?

# GXD HT Experiment
propTypeKey = 1002

# Number of experiments in GEO xml files
expCount = 0

# Number experiments loaded
exptLoadedCount = 0

# Number of samples loaded
sampleLoadedCount = 0

# Number of experiments in the db whose pubmed IDs were updated
updateExptCount = 0

# database lookups

# AE IDs in the database
# {primaryID:key, ...}
primaryIdDict = {}

# raw experiment types mapped to controlled vocabulary keys
exptTypeTransDict = {}

# pubmed properties by AE ID (primary) in the db
# {GEO ID: [pubmedId1, ..., pubmedIdn], ...}
pubMedByExptDict = {}

# experiment ids in the database skipped, primary skip
expIdsInDbSet = set()

# experiment types skipped because not in translation
expTypesSkippedSet = set()

# experiment ids not in database, type not translated, secondary skip
expSkippedNotInDbNoTransSet = set()

# experiments skipped because of 'Third-party reanalysis'
#tprSet = set()

# experiments skipped because > maxSamples
expSkippedMaxSamplesSet = set()

# experiments skipped because of sample parsing issues
expSkippedNoSampleList = []

# experiments loaded with no samples because no sample file
expLoadedNoSampleList = []

# all experiment ids 
allExptIdList = []

# skipped experiment ids keyed by reason

# experiment ids not in database, in translation. Is superseries
expSkippedNotInDbTransIsSuperseriesSet = set()

# global value  for sample Overall-Design value which is prepended
# to the experiment summary for the full experiment description
overallDesign = ''

#
# Purpose:  Open file descriptors, get next primary keys, create lookups
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: opens files, reads a database
# Throws: Nothing
#
def initialize():
    global fpQcFile, fpExpParsingFile, fpSampParsingFile, fpExperimentBcp 
    global fpSampleBcp, fpKeyValueBcp, pubMedByExptDict
    global fpAccBcp, fpVariableBcp, fpPropertyBcp, nextExptKey
    global nextAccKey, nextExptVarKey, nextPropKey
    global nextRawSampleKey, nextKeyValueKey

    # create file descriptors
    try:
        fpQcFile = open(qcFileName, 'w')
    except:
         print('Cannot create %s' % qcFileName)

    if runParsingReports == 'true':
        try:
            fpExpParsingFile = open(expParsingFileName, 'w')
        except:
             print('Cannot create %s' % expParsingFileName)

        try:
            fpSampParsingFile = open(sampParsingFileName, 'w')
        except:
             print('Cannot create %s' % sampParsingFileName)

    try:
        fpExperimentBcp = open('%s/%s' % (outputDir, experimentFileName), 'w')
    except:
        print('Cannot create %s' % (eFile))

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
        fpSampleBcp = open('%s/%s' % (outputDir, sampleFileName), 'w')
    except:
        print('Cannot create %s' % sampleFileName)

    try:
        fpKeyValueBcp = open('%s/%s' % (outputDir, keyValueFileName), 'w')
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

    # get next primary key for the Raw Sample table
    results = db.sql(''' select nextval('gxd_htrawsample_seq') as maxKey ''', 'auto')
    nextRawSampleKey = results[0]['maxKey']

    # get next primary key for the Key Value table
    results = db.sql(''' select nextval('mgi_keyvalue_seq') as maxKey ''', 'auto')
    nextKeyValueKey = results[0]['maxKey']

    # Create experiment ID lookup
    results = db.sql('''select accid, _Object_key
        from ACC_Accession
        where _MGIType_key = 42 -- GXD HT Experiment
        and _LogicalDB_key = 190 -- GEO Series''', 'auto')
    for r in results:
        primaryIdDict[r['accid']] = r['_Object_key']

    # Create experiment type translation lookup
    results = db.sql('''select badname, _Object_key
        from MGI_Translation
        where _TranslationType_key = 1020''', 'auto')
    for r in results:
        exptTypeTransDict[r['badname']] = r['_Object_key']

    # create the pubmed ID property lookup by experiment
    # This will be used in YAK-119 which includes updating pubmed ids
    results = db.sql('''select a.accid, p.value
        from GXD_HTExperiment e, ACC_Accession a, MGI_Property p
        where e._Experiment_key = a._Object_key
        and a._MGIType_key = %s
        and a._LogicalDB_key = %s
        and a.preferred = 1
        and e._Experiment_key = p._Object_key
        and p._PropertyTerm_key = %s
        and p._PropertyType_key = %s''' % \
            (exptMgiTypeKey, geoLdbKey, pubmedPropKey, propTypeKey), 'auto')

    for r in results:
        accid = r['accid'] 
        value = r['value']
        if accid not in pubMedByExptDict:
            pubMedByExptDict[accid] = []
        pubMedByExptDict[accid].append(value)

    db.useOneConnection(0)
    
    return

#
# Purpose: Loops through all experiment files sending them to parser
# Returns:
# Assumes: Nothing
# Effects:
# Throws: Nothing
#

def processAll():

    if runParsingReports == 'true':
        fpExpParsingFile.write('expID%ssampleList%stitle%ssummary+overall-design%sisSuperSeries%spdat%sChosen Expt Type%sn_samples%spubmedList%s' % (TAB, TAB, TAB, TAB, TAB, TAB, TAB, TAB, CRT))
        fpSampParsingFile.write('expID%ssampleID%sdescription%stitle%ssType%schannelInfo%s' % (TAB, TAB, TAB, TAB, TAB, CRT))

    for expFile in str.split(os.environ['EXP_FILES']):
        process(expFile)

    return

#
# Purpose: parse input file, QC, create bcp files
# Returns: 
# Assumes: globals have all been initialized
# Effects: Creates files in the file system
# Throws: Nothing
#

def process(expFile):
    global expCount, exptLoadedCount, updateExptCount
    global nextExptKey, nextAccKey, nextExptVarKey, nextPropKey
    global expSkippedNotInDbTransIsSuperseriesSet, expSkippedNoSampleList
    global expIdsInDbSet, expLoadedNoSampleList
    global expSkippedNotInDbNoTransSet, expSkippedMaxSamplesSet
    #global tprSet

    f = open(expFile, encoding='utf-8', errors='replace')   
    context = ET.iterparse(f, events=("start","end"))
    context = iter(context)
    
    level = 0
    expID = ''
    title = ''
    summary = ''
    pdat = ''
    gdsType = ''
    exptType = ''
    n_samples = ''
    pubmedList = []
    sampleList = [] # list of samplIDs

    isSuperSeries = 'no'   # flag to indicate expt is superseries, skip
    exptTypeKey = 0        # if 0 chosen gdstype did not translate, skip
    isTpr = 0
    for event, elem in context:
        # end of a record - reset everything
        if event=='end' and elem.tag == 'DocumentSummary':
            expCount += 1
            skip = 0
            print('\n\nexpID: %s' % expID)
            allExptIdList.append(expID)

            if expID in primaryIdDict:
                skip = 1
                expIdsInDbSet.add(expID)
                print('    expIdInDb skip')
                 # not all experiments have pubmed IDs
                if expID in pubMedByExptDict:
                    # get the list of pubmed Ids for this expt in the database
                    dbBibList = pubMedByExptDict[expID]

                    # get the set of incoming pubmed IDs not in the database
                    newSet = set(pubmedList).difference(set(dbBibList))

                    # if we have new pubmed IDs, add them to the database
                    if newSet:
                        print('found new pubmed ids: %s' % newSet)
                        updateExpKey = primaryIdDict[expID]

                        # get next sequenceNum for this expt's pubmed ID
                        # in the database
                        results = db.sql('''select max(sequenceNum) + 1
                            as nextNum
                            from MGI_Property p
                            where p._Object_key =  %s
                            and p._PropertyTerm_key = 20475430
                            and p._PropertyType_key = 1002''' % updateExpKey, 'auto')

                        nextSeqNum = results[0]['nextNum']

                        updateExptCount += 1

                        for b in newSet:
                            toLoad = propertyUpdateTemplate.replace('#=#', str(pubmedPropKey)).replace('#==#', str(b)).replace('#===#', str(nextSeqNum)).replace('#====#', str(nextPropKey)).replace('#=====#', str(updateExpKey))
                            fpPropertyBcp.write(toLoad)
                            nextPropKey += 1
                # continue so we don't dup what is in the db
                #continue this is handled by 'skip'


            typeList = list(map(str.strip, gdsType.split(';')))
            #if skip != 1 and 'Third-party reanalysis' in typeList:
            #        tprSet.add(expID)
            #        print("ExpID: %s is 'Third-party reanalysis'" % expID)
            #        print('TypeList: %s' % typeList)
            #        skip = 1
            #        print('    tprSet.add skip')

            if skip != 1:
                (exptTypeKey, exptType) = processExperimentType(typeList)
                if exptTypeKey == 0:
                    # expts whose type doesn't translate and is not already in the db
                    expSkippedNotInDbNoTransSet.add(expID)
                    skip = 1
                    print('    expIdNotInDbNoTrans skip')

            if skip != 1 and isSuperSeries == 'yes':
                # number of superseries not already caught because of un translated
                # exptType or already in DB
                expSkippedNotInDbTransIsSuperseriesSet.add(expID)
                skip = 1

            if skip != 1 and int(n_samples) > int(maxSamples):
                expSkippedMaxSamplesSet.add(expID)
                skip = 1
            #print('exptTypeKey: %s isSuperSeries: %s skip: %s' % (exptTypeKey, isSuperSeries, skip))
            if  skip != 1: 
                exptLoadedCount += 1
                createExpObject = 0
                # now process the samples
                ret =  processSamples(expID)
                #print('ret: %s' % ret)
                if ret == 1:
                    expLoadedNoSampleList.append('expID: %s' % (expID))
                    createExpObject = 1
                elif ret == 2:
                    expSkippedNoSampleList.append('expID: %s' % (expID))
                    exptLoadedCount -= 1 # decrement the loaded count
                else:
                    sampleList = ret #  list of sampleString's representing each
                                     #  sample for the current experiment
                    createExpObject = 1
                if createExpObject:
                    # catenate the global overallDesign parsed from the sample to the
                    # experiment summary
                    description = '%s %s' % (summary, overallDesign)
                    description = description.replace('\t', ' ')
                    description = description.replace('\n', ' ')

                    if runParsingReports == 'true':
                       fpExpParsingFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (expID, TAB, ', '.join(sampleList), TAB, title, TAB, description, TAB, isSuperSeries, TAB, pdat, TAB, exptType, TAB, n_samples, TAB, ', '.join(pubmedList), CRT) )

                    #
                    # GXD_HTExperiment BCP
                    #

                    line = '%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextExptKey, TAB, sourceKey, TAB, title, TAB, description, TAB, pdat, TAB, releasedate, TAB, evalDate, TAB, evalStateTermKey, TAB, curStateTermKey, TAB, studyTypeTermKey, TAB, exptTypeKey, TAB, evalByKey, TAB, initCurByKey, TAB, lastCurByKey, TAB, initCurDate, TAB, lastCurDate, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT) 
                    #print('line: %s' % line)
                    fpExperimentBcp.write(line)
           
                    #
                    # GXD_HTVariable BCP
                    #
                    fpVariableBcp.write('%s%s%s%s%s%s' % (nextExptVarKey, TAB, nextExptKey, TAB, exptVariableTermKey, CRT))
                    nextExptVarKey += 1

                    #
                    # ACC_Accession BCP
                    #
                    prefixPart, numericPart = accessionlib.split_accnum(expID)
                    fpAccBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextAccKey, TAB, expID, TAB, prefixPart, TAB, numericPart, TAB, geoLdbKey, TAB, nextExptKey, TAB, exptMgiTypeKey, TAB, private, TAB, isPreferred, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT ))
                    nextAccKey += 1

                    #
                    # Experiment Properties
                    #

                    # title (1) experiment name 
                    #   namePropKey = 20475428
                    # n_samples (1) count of samples  
                    #   sampleCountPropKey = 20475424
                    # typeList (1-n) raw experiment types 
                    #   expTypePropKey = 20475425
                    # pubmedList (0-n) pubmed Ids 
                    #   pubmedPropKey = 20475430
                    # description (1) sample overalldesign + expt summary 
                    #   descriptionPropKey = 87508020

                    # the template for properties:
                    propertyTemplate = "#====#%s%s%s#=#%s%s%s%s%s#==#%s#===#%s%s%s%s%s%s%s%s%s" % (TAB, propTypeKey, TAB, TAB, nextExptKey, TAB, exptMgiTypeKey, TAB, TAB, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT )


                    if title != '':
                        toLoad = propertyTemplate.replace('#=#', str(namePropKey)).replace('#==#', title).replace('#===#', '1').replace('#====#', str(nextPropKey))
                        fpPropertyBcp.write(toLoad)
                        nextPropKey += 1

                    if n_samples != '':
                        toLoad = propertyTemplate.replace('#=#', str(sampleCountPropKey)).replace('#==#', str(n_samples)).replace('#===#', '1').replace('#====#', str(nextPropKey))
                        fpPropertyBcp.write(toLoad)
                        nextPropKey += 1

                    seqNumCt = 1
                    for e in typeList:
                        toLoad = propertyTemplate.replace('#=#', str(expTypePropKey)).replace('#==#', e).replace('#===#', str(seqNumCt)).replace('#====#', str(nextPropKey))
                        fpPropertyBcp.write(toLoad)
                        seqNumCt += 1
                        nextPropKey += 1

                    for b in pubmedList:
                        toLoad = propertyTemplate.replace('#=#', str(pubmedPropKey)).replace('#==#', str(b)).replace('#===#', str(seqNumCt)).replace('#====#', str(nextPropKey))
                        fpPropertyBcp.write(toLoad)
                        seqNumCt += 1
                        nextPropKey += 1

                    if title != '':
                        toLoad = propertyTemplate.replace('#=#', str(namePropKey)).replace('#==#', title).replace('#===#', '1').replace('#====#', str(nextPropKey))
                        fpPropertyBcp.write(toLoad)
                        nextPropKey += 1


                    #if description != '':
                    #    toLoad = propertyTemplate.replace('#=#', str(descriptionPropKey)).replace('#==#', str(description)).replace('#===#', '1').replace('#====#', str(nextPropKey))
                    #    fpPropertyBcp.write(toLoad)
                    #    nextPropKey += 1

                    
                    #
                    # GXD_HTRawSample and MGI_KeyValue BCP
                    #
                    # ret from processSample = 1 means there was no sample file
                    # so exeriment is created, but no samples
                    if ret != 1:
                        processSampleBcp(sampleList, nextExptKey) 

                    # now increment the experiment key
                    nextExptKey += 1
            title = ''
            summary = ''
            isSuperSeries = 'no'
            pdat = ''
            gdsType = ''
            n_samples = ''
            pubmedList = []
            sampleList = []
            exptTypeKey = 0
        
        if level == 4 : 
            # Accession tag at level 4 tells us we have a new record
            if elem.tag == 'Accession':
                expID = elem.text
            elif elem.tag == 'title':
                title = elem.text
            elif elem.tag == 'summary':    
                summary = elem.text
                if summary.find(SUPERSERIES) != -1:
                    isSuperSeries =  'yes'
                    #print('isSuperSeries: %s' % expID)
            elif elem.tag == 'PDAT':
                pdat = elem.text
            elif elem.tag == 'gdsType':
                gdsType = elem.text
            elif elem.tag == 'n_samples':
                n_samples = elem.text
        if event=='start':
            level += 1
            #print('level: %s elemTag: %s elemText: %s' % (level, elem.tag, elem.text))
        elif elem.tag == 'int':
            id = elem.text
            #print('id: %s' % id)
            pubmedList.append(id)
        elif level == 6 and elem.tag == 'Accession':
            sampleList.append(elem.text)
        if event == 'end':
            level -= 1
    
    elem.clear()

    return

#
# Purpose: looks a the list of expt types and determines if
#       there is one in the expt translation. 
# Returns: expt type and expt type key of the chosen expt type; both 0 if
#       no translatable expt type
# Assumes: exptTypeTransDict has been initialized
# Effects: 
# Throws: Nothing
#

def processExperimentType(typeList):
     global expTypesSkippedSet

     # pick first valid experiment type and translate it
     exptTypeKey = 0
     found = 0
     for exptType in typeList:
         if exptType not in exptTypeTransDict:
             expTypesSkippedSet.add(exptType)
         # we take the FIRST translatable expt type in the list
         # but we want to iterate through all types to get all
         # that do not translate i.e. don't use i'break'
         elif exptType in exptTypeTransDict and found == 0:
             exptTypeKey= exptTypeTransDict[exptType]
             found = 1
     return (exptTypeKey, exptType)


#
# Purpose: parses the sample file for 'expID' if it exists
# Returns: 1 if the sample file does not exist, 2 if parsing errors, 
#       otherwise sampleList. sampleList is a list of strings of sample
#       metadata, one for each sample
# Assumes: Nothing
# Effects: 
# Throws: Nothing
#

def processSamples(expID):
    global overallDesign

    #print('expID: %s' % (expID))
    sampleFile = '%s%s' % (expID, sampleFileSuffix)
    samplePath = '%s/%s' % (geoDownloads, sampleFile)
    #print(samplePath)

    # if sample file does not exist return 1
    if not os.path.exists(samplePath):
        return 1

    # list of samples for this experiment to return
    sampleList = []

    f = open(samplePath, encoding='utf-8', errors='replace')
    context = ET.iterparse(f, events=("start","end"))
    context = iter(context)

    level = 0
    sampleID = ''
    description = ''
    title = ''
    sType = ''
    molecule = ''
    taxid = ''
    taxidValue = ''
    treatmentProt = ''
    overallDesign = ''
    channelDict = ''
    channelList = ''
    # dictionary of key/values for the Channel section
    channelDict = {}

    # There can be 1 or 2 channels (not yet sure if there can be zero
    # first dict in list is channel 1, second is channel 2 (if there is one)
    channelList = []

    # Channel, there can be 1 or 2, need for sequence of sets of 
    # source/taxid/treatment/molecule
    cCount = 0 

    #
    # Parse the sample file
    #
    #try:
    for event, elem in context:
        if event == 'start':
            level += 1
        if event == 'end':
            level -= 1
        #
        # we are done processing a sample, print and reset
        #
        if event == 'end' and elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Sample':
            # if the dict is not empty, add it to the list
            if channelDict:
                #print(channelDict)
                #print('set second channel in channelList')
                channelList.append(channelDict)

            # process the 1 or 2 channels for the current sample
            channelString = processChannels(channelList)

            # the string of attributes representing the current sample
            sampleString = ('%s%s%s%s%s%s%s%s%s%s%s' % (expID, TAB, sampleID, TAB, description, TAB, title, TAB, sType, TAB, channelString))

            # append to the list of attributes for each sample in this 
            # experiment
            sampleList.append(sampleString)

            if runParsingReports == 'true':
                fpSampParsingFile.write('%s%s' % (sampleString, CRT))
            #
            # reset all attributes
            #
            sampleID = ''
            description =  ''
            title = ''
            sType = ''
            molecule = ''
            taxid = ''
            taxidValue = ''
            treatmentProt = ''
            overallDesign = ''
            channelDict = {}
            channelList = []

        #
        # Tag Level 2
        #
        if level == 2:
            if elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Sample':
                sampleID = str.strip(elem.get('iid'))
            elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Overall-Design':
                if overallDesign == None:
                    overallDesign = ''
                else:
                    overallDesign = ((str.strip(elem.text)).replace(TAB, '')).replace(CRT, '')


        #
        # Tag Level 3
        #

        if level == 3:
            if elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Description':
                description = elem.text
                if description == None:
                    description = ''
                else:
                    description = ((str.strip(description)).replace(TAB, '')).replace(CRT, '')
            elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Title':
                title = elem.text
                if title == None:
                    title = ''
                else:
                    title = ((str.strip(title)).replace(TAB, '')).replace(CRT, '')
            elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Type':
                sType = elem.text
                if sType == None:
                    sType = ''
                else:
                    sType = ((str.strip(sType)).replace(TAB, '')).replace(CRT, '')
            elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Channel':
                cCount = int(elem.get('position'))
                # if we have a second channel, append the first to the 
                # List and reset the dict
                if cCount == 2:
                    channelList.append(channelDict)
                    channelDict = {}

            elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Characteristics':
                tag = elem.get('tag')
                if tag == None: # not an attrib just get the text
                    tag = 'Characteristics' # name it

                # strip and replace internal tabs and crt's
                tag = ((str.strip(tag)).replace(TAB, '')).replace(CRT, '') 
                value = ((str.strip(elem.text)).replace(TAB, '')).replace(CRT, '') 
                print('expID: %s sampleID: %s tag: %s value: %s' % \
                    (expID, sampleID, tag, value))
                if value is not None and value != '':
                    channelDict[tag] = value


        #
        # Tag Level 4
        #

        if level == 4:
            if elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Source':
                source = elem.text
                if source is not None and source != '':
                    channelDict['source'] = str.strip(source)
            elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Organism':
                taxid = elem.get('taxid')
                if taxid is not None and taxid != '':
                    channelDict['taxid'] = str.strip(taxid)
                taxidValue = elem.text
                if taxidValue is not None and taxidValue != '':
                    channelDict['taxidValue'] = str.strip(taxidValue)

            elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Treatment-Protocol':
                treatmentProt = elem.text
                #if treatmentProt[-1] == '\\':
                #    treatmentProt = treatmentProt[0:-1]
                print('expID: %s sampleID: %s treatmentProt: "%s"' % (expID, sampleID, treatmentProt))
                if treatmentProt is not None and treatmentProt != '':
                    treatmentProt = ((str.strip(treatmentProt)).replace(TAB, '')).replace(CRT, '')
                    print('adding to channelDict expID: %s sampleID: %s treatmentProt: %s' % (expID, sampleID, treatmentProt))
                    channelDict['treatmentProt'] = treatmentProt
            elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Molecule':
                molecule = elem.text
                if molecule is not None and molecule != '':
                        channelDict['molecule'] = str.strip(molecule)
    #except: # there was a parsing error
    #    return 2

    return sampleList

#
# Purpose: creates a string representation of channel metadata in channelList
# Returns: empty string if no channels or string of channel data pipe delimited
#       if two channels
# Assumes: Nothing
# Effects:
# Throws: Nothing
#

def processChannels(channelList): # list of 1 or 2 dictionaries of key/value
    # no channels in this sample, return empty string
    if not channelList:
        return ''
    if len(channelList) == 1:
        return processOneChannel(channelList[0])
    else:
        string1 = processOneChannel(channelList[0])
        string2 = processOneChannel(channelList[1])
        return '%s|||%s' % (string1, string2)

#
# Purpose: processes one dictionary of channel metadata, delimiting key/value
#       with':::', delimiting each key/value with '!!!'
# Returns: string representing channel metadata for one channel
# Assumes: Nothing
# Effects:
# Throws: Nothing
#

def processOneChannel(channelDict):
    keyValueList = []
    for key in channelDict:
        keyValueList.append('%s:::%s' % (key, channelDict[key]))
    return '!!!'.join(keyValueList)

#
# Purpose: parses a list of sample strings for a single experiment
#       and writes to bcp file for gxd_htrawsample and mgi_keyvalue
#       for the channel data
# Returns: 
# Assumes: Nothing
# Effects: increments the global raw sample and key value primary keys
# Throws: Nothing
#

def processSampleBcp(sampleList, # list of samples for current experiment
                        nextExptKey): # expt key for samples we are processing

    global nextRawSampleKey, nextKeyValueKey, sampleLoadedCount

    #
    # sampleString looks like:
    # (expID, TAB, sampleID, TAB, description, TAB, title, TAB, sType, 
    #    TAB, channelString)
    #
    # channelString looks like:
    # '!!!' delim key:value,  '|||' delim channel
    #
    for sampleString in sampleList:
        sampleLoadedCount += 1
        print('sampleString: %s' % sampleString)
        expID, sampleID, description, title, sType, channelString = str.split(sampleString, TAB) 

        # write to fpSampleBcp here
        fpSampleBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextRawSampleKey, TAB, nextExptKey, TAB, sampleID, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT))

        channels = str.split(channelString, '|||')
        seqNum = 1 # there can be 1 or 2 channels, data for each channel
                   # distinguished by seqNum

        for channel in channels:
            tokens = str.split(channel, '!!!')
            print('tokens: %s' % tokens)
            for keyValue in tokens:
                print('keyValue: %s' % keyValue)
                key, value = str.split(keyValue, ':::')
                if key == 'antibody':
                    print('raw value: %s' % value)
                value = value.replace('\\', '\\\\')
                value = value.replace('#', '\#')
                value = value.replace('?', '\?')
                value = value.replace('\n', '\\n')
                #value = value.replace('μ', '')
                #value = value.replace('µ', '')
                #value = value.replace('′', '')
                key = key.replace('\\', '\\\\')
                key = key.replace('#', '\#')
                key = key.replace('?', '\?')
                key = key.replace('\n', '\\n')
                #key = key.replace('μ', '')
                #key = key.replace('µ', '')
                #key = key.replace('′', '')

                # replace em-dash with two en-dash
                value = value.replace(b'\xe2\x80\x94'.decode('utf-8'), '--')
                key = key.replace(b'\xe2\x80\x94'.decode('utf-8'), '--')
                if key == 'antibody':
                    print('value after replace: "%s"' % value)
                print('value: %s' % value)
                # this removes non-ascii characters; 
                # 'replace' replaces with '?'
                #value_encode = value.encode('ascii', 'ignore')
                #key_encode = key.encode('ascii', 'ignore')
                value_encode = value.encode('ascii', 'replace')
                key_encode = key.encode('ascii', 'replace')
                print('value_encode: %s' % value_encode)
                print('key_encode: %s' % key_encode)
                value_decode = value_encode.decode()
                key_decode = key_encode.decode()
                print('value_decode: %s' % value_decode)
                print('key_decode: %s' % key_decode)

                if value_decode == '-' or value_decode == '' or value_decode is None:
                    print('updated to --')
                    value_decode = '--'
                print('new value_decode for %s: %s' % (key_decode, value_decode))
                # write to fpKeyValueBcp here
                fpKeyValueBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextKeyValueKey, TAB, nextRawSampleKey, TAB, rawSampleMgiTypeKey, TAB, key_decode, TAB, value_decode, TAB, seqNum, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT))
                nextKeyValueKey += 1     
            # increment, there may be a second channel
            seqNum += 1 

        # increment the sample key, multiple samples/experiment
        nextRawSampleKey += 1

    return

#
# Purpose: writes out QC to the QC file
# Returns: 
# Assumes: Nothing
# Effects:
# Throws: Nothing
#

def writeQC():
    #global expTypesSkippedSet, expIdsInDbSet

    fpQcFile.write('GEO HT Raw Data Load QC%s%s%s' % (CRT, CRT, CRT))

    fpQcFile.write('Number of experiments in the input: %s%s%s' % \
        (expCount, CRT, CRT))

    fpQcFile.write('Number of experiments loaded: %s%s%s' % \
        (exptLoadedCount, CRT, CRT))

    fpQcFile.write('Number of samples loaded: %s%s%s' % \
        (sampleLoadedCount, CRT, CRT))

    fpQcFile.write('Number experiments skipped, already in DB: %s%s' %\
        (len(expIdsInDbSet), CRT))
    for id in  expIdsInDbSet:
        fpQcFile.write('    %s%s' %  (id, CRT))

    #fpQcFile.write('%sNumber experiments skipped, not already in db. Is Third-party reanalysis: %s%s' % \
    #    (CRT, len(tprSet),  CRT))
    #for id in  tprSet:
    #    fpQcFile.write('    %s%s' %  (id, CRT))
       
    #fpQcFile.write('%sNumber experiments skipped, not already in db, not Third-party reanalysis. Type not in translation: %s%s%s' % \
    fpQcFile.write('%sNumber experiments skipped, not already in db. Type not in translation: %s%s%s' % \
        (CRT, len(expSkippedNotInDbNoTransSet), CRT, CRT))
    for id in  expSkippedNotInDbNoTransSet:
        fpQcFile.write('    %s%s' %  (id, CRT))

    # fpQcFile.write('%sNumber experiments skipped, not already in db, not Third-party reanalysis, type not in translation. Is SuperSeries: %s%s%s' % \
    fpQcFile.write('%sNumber experiments skipped, not already in db, type not in translation. Is SuperSeries: %s%s%s' % \
        (CRT, len(expSkippedNotInDbTransIsSuperseriesSet), CRT, CRT))
    for id in  expSkippedNotInDbTransIsSuperseriesSet:
        fpQcFile.write('    %s%s' %  (id, CRT))

    # fpQcFile.write('%sNumber experiments skipped, not already in db, not Third-party reanalysis, type not in translation, is not SuperSeries, has > max samples: %s%s%s' % \
    fpQcFile.write('%sNumber experiments skipped, not already in db, type not in translation, is not SuperSeries, has > max samples: %s%s%s' % \
        (CRT, len(expSkippedMaxSamplesSet), CRT, CRT))
    for id in expSkippedMaxSamplesSet:
        fpQcFile.write('    %s%s' %  (id, CRT))

    fpQcFile.write('%sNumber experiments skipped because of Sample parsing issues: %s%s' % (CRT, len(expSkippedNoSampleList), CRT))
    for e in expSkippedNoSampleList:
        fpQcFile.write('    %s%s' %  (e, CRT))   

    fpQcFile.write('%sNumber experiments loaded w/o samples, no sample file: %s%s' % (CRT, len(expLoadedNoSampleList), CRT))
    for e in expLoadedNoSampleList:
        fpQcFile.write('    %s%s' %  (e, CRT))

    fpQcFile.write('%sSet of unique GEO Experiment Types not found in Translation: %s%s' % (CRT, len(expTypesSkippedSet), CRT))
    sortedSet = sorted(expTypesSkippedSet)
    for type in sortedSet:
        fpQcFile.write('    %s%s' %  (type, CRT))

    return

#
# Purpose: executes bcp
# Returns: non-zero if bcp error, else 0
# Assumes:
# Effects: executes bcp, writes to the database
# Throws: Nothing
#

def doBCP():
    
    bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, expt_table, outputDir, experimentFileName)
    print('bcpCmd: %s' % bcpCmd)
    rc = os.system(bcpCmd)

    if rc:
        return rc

    bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, acc_table, outputDir, accFileName)
    print('bcpCmd: %s' % bcpCmd)
    rc = os.system(bcpCmd)

    if rc:
        return rc

    bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, exptvar_table, outputDir, variableFileName)
    print('bcpCmd: %s' % bcpCmd)
    rc = os.system(bcpCmd)

    if rc:
        return rc

    bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, property_table, outputDir, propertyFileName)
    print('bcpCmd: %s' % bcpCmd)
    rc = os.system(bcpCmd)

    bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, sample_table, outputDir, sampleFileName)
    print('bcpCmd: %s' % bcpCmd)
    rc = os.system(bcpCmd)

    if rc:
        return rc

    bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, keyvalue_table, outputDir, keyValueFileName)
    print('bcpCmd: %s' % bcpCmd)
    rc = os.system(bcpCmd)

    if rc:
        return rc

    # update gxd_htrawsample_seq auto-sequence
    db.sql(''' select setval('gxd_htrawsample_seq', (select max(_RawSample_key) from GXD_HTRawSample)) ''', None)

    # update mgi_keyvalue_seq auto-sequence
    db.sql(''' select setval('gxd_htrawsample_seq', (select max(_KeyValue_key) from MGI_KeyValue)) ''', None)

    if rc:
        return rc

    return 0
#
# Purpose: Close file descriptors
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: Closes files
# Throws: Nothing
#
def closeFiles():

    fpQcFile.close()
    fpExpParsingFile.close()
    fpSampParsingFile.close()
    fpExperimentBcp.close()
    fpSampleBcp.close()
    fpKeyValueBcp.close()
    fpAccBcp.close()
    fpVariableBcp.close()
    fpPropertyBcp.close()

    return
#
# main
#

initialize()
processAll()
writeQC()
closeFiles()
doBCP()
