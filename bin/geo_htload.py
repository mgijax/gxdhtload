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
#   insert into mgi_translation(select nextval('mgi_translation_seq'),1020,20475438,'Other',19,1001,1001,now(),now())
#
# lec   02/18/2025
#   wts2-1616/E4G-150/GEO GXD Ht Load: dropping and reloading of raw samples not happening
#   moved delete of Raw Sample from "by sample name" -> "by experiment key"
#
# sc   06/17/2021
#       - created WTS2-431
#
'''
import os
import sys
import types
import re
import Set
import db
import loadlib
import accessionlib
import xml.etree.ElementTree as ET
from datetime import date

TAB = '\t'
CRT = '\n'

# default experiment confidence value
confidence = 0.0

# load only experiments with <= MAX_SAMPLES
maxSamples = int(os.environ['MAX_SAMPLES'])

# debug true/false
DEBUG = os.environ['LOG_DEBUG']

# string that identifies supereries which we will not load
SUPERSERIES='This SuperSeries'

# today's date
loadDate = loadlib.loaddate

# user creating the database records, gxdhtload
# used for record createdBy and modifiedBy
userKey = 1626 

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

#
# For GXD_HTExperimentVariable:
#

# 'Not Curated' from 'GXD HT Variables' (vocab key=122)
exptVariableTermKey = 20475439 

#
# For GXD_HTExperiment:
#

# 'Not Evaluated' from 'GXD HT Evaluation State' (vocab key = 116) 
evalStateTermKey = 100079348

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
evalDate = ''

# 'GEO' from 'GXD HT Source' (vocab key = 119)
sourceKey = 87145238

#
# For GXD_HTRawSample
#

rawSampleMgiTypeKey = 47

#
# File Descriptors:
#

# to form the sample file to process
geoDownloads = os.environ['GEO_DOWNLOADS']
sampleFileSuffix = os.environ['GEO_SAMPLE_FILE_SUFFIX']

# QC file and descriptor
today = date.today()
suffix  = '%s.rpt' % today.strftime("%b-%d-%Y")

qcFileName = '%s.%s' % (os.environ['QC_RPT'], suffix)
fpQcFile = None

# report of gained/lost samples for curated experiments
curatedQcFileName = '%s.%s' % (os.environ['CURATED_QC_RPT'], suffix)
fpCuratedQcFile = None

# Experiment parsing reports
expParsingFileName = os.environ['EXP_PARSING_RPT']
fpExpParsingFile = None

# Sample parsing report
sampParsingFileName  = os.environ['SAMP_PARSING_RPT']
fpSampParsingFile = None
sampInDbParsingFileName = os.environ['SAMP_IN_DB_PARSING_RPT']
fpSampInDbParsingFile = None

# run parsing reports true/false
runParsingReports = os.environ['RUN_PARSING_RPTS']

#
# For bcp 
#

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

#
# sql file for deleting samples
#
deleteFileName = os.environ['DELETE_FILENAME']
fpSampleDelete = None
deleteTemplate = '''delete from GXD_HTRawSample where _experiment_key = %s;\n'''

#
# for MGI_Property
#

# GXD HT Experiment Property vocab
expTypePropKey = 20475425       # raw experiment type
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

# Experiments in the db whose pubmed IDs were updated
updateExptList = []

#
# database lookups
#

# experiments with curated samples
# {exptID:[raw sample ids], ...}
curatedExptDict = {}

nonCuratedExptDict = {}

# GEO IDs in the database
# {exptID:key, ...}
geoExptInDbDict = {}

# raw experiment types mapped to controlled vocabulary keys
exptTypeTransDict = {}

# pubmed properties by AE ID (primary) in the db
# {GEO ID: [pubmedId1, ..., pubmedIdn], ...}
pubMedByExptDict = {}

# experiment IDs in the input found to be in the database
expIdsInDbSet = set()

#
# data structures for load QC reporting
#

# experiment types skipped because not in translation
expTypesSkippedSet = set()

# experiment ids not in database, type not translated, secondary skip
expSkippedNotInDbNoTransSet = set()

# experiments, new and existin, that have > maxSamples
expMaxSamplesSet = set()

# experiments skipped because of sample parsing issues
expSkippedNoSampleList = []

# experiments loaded with no samples because no sample file
expLoadedNoSampleList = []

# {exptID:[sampleId1, ...sampleIdn], ...}
duplicatedSampleIdDict = {}

# experiment ids not in database, in translation. Is superseries
expSkippedNotInDbTransIsSuperseriesSet = set()

# QC report lines for curated experiments with raw sample gains/losses
curSampleGainLossList = []
ncSampleGainLossList = [] # not curated

# This for debugging the difference in # sampled deleted vs reloaded
# number of samples lost
lostCt = 0
lostSampleDict = {} # {exptID:(set of sampleIDs), ...}

# number of samples gained
gainedCt = 0
gainedSampleDict = {}

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
    global fpQcFile, fpExpParsingFile, fpSampParsingFile, fpSampInDbParsingFile
    global fpExperimentBcp, fpSampleBcp, fpKeyValueBcp, fpSampleDelete
    global fpAccBcp, fpVariableBcp, fpPropertyBcp, nextExptKey
    global nextAccKey, nextExptVarKey, nextPropKey, geoExptInDbDict
    global pubMedByExptDict, nextRawSampleKey, nextKeyValueKey 
    global curatedExptDict, nonCuratedExptDict
    global fpCuratedQcFile

    # create file descriptors
    try:
        fpQcFile = open(qcFileName, 'w')
    except:
         print('Cannot create %s' % qcFileName)

    try:
        fpCuratedQcFile = open(curatedQcFileName, 'w')
    except:
         print('Cannot create %s' % curatedQcFileName)


    if runParsingReports == 'true':
        try:
            fpExpParsingFile = open(expParsingFileName, 'w')
        except:
             print('Cannot create %s' % expParsingFileName)

        try:
            fpSampInDbParsingFile = open(sampInDbParsingFileName, 'w')
        except:
             print('Cannot create %s' % sampInDbParsingFileName)

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

    try:
        fpSampleDelete = open('%s' % (deleteFileName), 'w')
    except:
        print('Cannot create %s' % deleteFileName)

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

    # Create experiment ID lookup - we are looking at preferred = 0 or 1 
    # because GEO ids can be either
    db.sql('''select accid, _Object_key as _experiment_key
        into temporary table geo
        from ACC_Accession
        where _MGIType_key = 42 -- GXD HT Experiment
        and _LogicalDB_key = 190 -- GEO Series''', None)
    results = db.sql('''select * from  geo''', 'auto')

    for r in results:
        geoExptInDbDict[r['accid']] = r['_experiment_key']

    # Create experiment type translation lookup
    results = db.sql('''select badname, _Object_key from MGI_Translation where _TranslationType_key = 1020''', 'auto')
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
        and p._PropertyType_key = %s
        union all
        select a.accid, p.value
        from GXD_HTExperiment e, ACC_Accession a, MGI_Property p
        where e._Experiment_key = a._Object_key
        and a._MGIType_key = %s
        and a._LogicalDB_key = %s
        and a.preferred = 0
        and e._Experiment_key = p._Object_key
        and p._PropertyTerm_key = %s
        and p._PropertyType_key = %s''' % \
            (exptMgiTypeKey, geoLdbKey, pubmedPropKey, propTypeKey, exptMgiTypeKey, geoLdbKey, pubmedPropKey, propTypeKey), 'auto')

    for r in results:
        accid = r['accid'] 
        value = r['value']
        if accid not in pubMedByExptDict:
            pubMedByExptDict[accid] = []
        pubMedByExptDict[accid].append(value)

    # get curated experiments
    db.sql('''select distinct a.accid as exptID, a._object_key as _experiment_key
        into temporary table curated
        from acc_accession a, gxd_htsample s
                where a._MGIType_key = 42 -- GXD HT Experiment
                and a._LogicalDB_key = 190 -- GEO Series
        and a._object_key = s. _experiment_key''', None)

    db.sql('''create index idx_curated on curated(_experiment_key)''')

    # bring in the raw samples
    results = db.sql('''select c.*, rs.accid as rsID
        from curated c, gxd_htrawsample rs
        where c._experiment_key = rs._experiment_key''', 'auto')

    for r in results:
        exptID = r['exptID']
        rsID = r['rsID']
        if exptID not in curatedExptDict:
            curatedExptDict[exptID] = []
        curatedExptDict[exptID].append(rsID)
       
    results = db.sql('''select distinct a.accid as exptID, a._object_key as _experiment_key, rs.accid as rsID
        from acc_accession a
left outer join gxd_htrawsample rs on (a._object_key = rs._experiment_key)
                where a._MGIType_key = 42 -- GXD HT Experiment
                and a._LogicalDB_key = 190 -- GEO Series ''', 'auto')
    for r in results:
        exptID = r['exptID']
        rsID = r['rsID']
        if exptID not in nonCuratedExptDict:
            nonCuratedExptDict[exptID] = []
        nonCuratedExptDict[exptID].append(rsID)

    db.useOneConnection(0)
    
    return 0

#
# Purpose: removes all non-ascii characters from 'text'
#          also remove embedded newline and line feed
# Returns: 'text' with ascii chars removed
# Assumes: Nothing
# Effects:
# Throws: Nothing
#

def removeNonAscii(text):

    text = ((str.strip(text)).replace(TAB, ' ')).replace(CRT, ' ')
    newText = ''
    for c in text:
        if ord(c) < 128:
            newText += c

    return newText

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
        fpSampInDbParsingFile.write('expID%ssampleID%sdescription%stitle%ssType%schannelInfo%s' % (TAB, TAB, TAB, TAB, TAB, CRT))
    for expFile in str.split(os.environ['EXP_FILES']):
        print('processing: %s' % expFile)
        rc = process(expFile)

    return rc

#
# Purpose: parse input file, QC, create bcp files
# Returns: 
# Assumes: globals have all been initialized
# Effects: Creates files in the file system
# Throws: Nothing
#

def process(expFile):
    global expCount, exptLoadedCount, updateExptList
    global nextExptKey, nextAccKey, nextExptVarKey, nextPropKey
    global expSkippedNotInDbTransIsSuperseriesSet, expSkippedNoSampleList
    global expIdsInDbSet, expLoadedNoSampleList
    global expSkippedNotInDbNoTransSet, expMaxSamplesSet

    f = open(expFile, encoding='utf-8', errors='replace')   
    #f = open(expFile, encoding='latin-1', errors='replace')
    context = ET.iterparse(f, events=("start","end"))
    context = iter(context)
      
    level = 0
    expID = ''
    title = ''
    summary = ''
    pdat = ''
    gdsType = ''
    exptType = ''
    n_samples = '' # this used to test for max samples, no longer stored
    pubmedList = []
    sampleList = [] # list of sampleIDs

    isSuperSeries = 'no'   # flag to indicate expt is superseries, skip
    exptTypeKey = 0        # if 0 chosen gdstype did not translate, skip
    isTpr = 0
    
    for event, elem in context:
        # end of a record - reset everything
        if event=='end' and elem.tag == 'DocumentSummary':
            if DEBUG == 'true':
                print('expID: %s' % expID)
            expCount += 1
            skip = 0

            #
            # Experiment is in the database
            # add new pubmed ids
            # reload sample data
            #
            if expID in geoExptInDbDict:
                updateExpKey = geoExptInDbDict[expID]
                #
                # check for additional pubmed IDs
                #
                propertyUpdateTemplate = "#====#%s%s%s#=#%s#=====#%s%s%s#==#%s#===#%s%s%s%s%s%s%s%s%s" % (TAB, propTypeKey, TAB, TAB, TAB, exptMgiTypeKey, TAB, TAB, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT )
                skip = 1
                expIdsInDbSet.add(expID)
                if DEBUG == 'true':
                    print('expIdInDb skip')

                # not all experiments have pubmed IDs in the database
                # assigning empty list assures we pick up this case
                dbBibList = []

                if expID in pubMedByExptDict:
                    # get the list of pubmed Ids for this expt in the database
                    dbBibList = pubMedByExptDict[expID]

                # get the set of incoming pubmed IDs not in the database
                newSet = set(pubmedList).difference(set(dbBibList))

                # if we have new pubmed IDs, add them to the database
                if newSet:

                    # get next sequenceNum for this expt's pubmed ID
                    # in the database

                    # get the next property sequence number
                    results = db.sql('''select max(sequenceNum) + 1
                        as nextNum
                        from MGI_Property p
                        where p._Object_key =  %s
                        and p._PropertyTerm_key = 20475430
                        and p._PropertyType_key = 1002''' % updateExpKey, 'auto')
                    nextSeqNum = results[0]['nextNum']

                    if nextSeqNum == None:
                        nextSeqNum = 1

                    updateExptList.append(expID)

                    for b in newSet:
                        toLoad = propertyUpdateTemplate.replace('#=#', str(pubmedPropKey)).replace('#==#', str(b)).replace('#===#', str(nextSeqNum)).replace('#====#', str(nextPropKey)).replace('#=====#', str(updateExpKey))
                        fpPropertyBcp.write(toLoad)
                        nextPropKey += 1

                # if there's no raw sample data for the existing experiment, add it
                if DEBUG == 'true':
                    print('processing samples')
                ret =  processSamples(expID, 'true') # 1, 2 or a list of sample info
                if ret == 1:
                     print('returnCode for %s: %s, no sample file' % (expID, ret))
                elif ret == 2:
                     print('returnCode for %s: %s, parsing issue' % (expID, ret))
                else:
                     # wts2-1339: expt is in db, call processSampleBcp only if <= maxSamples
                     # only add samples if <= the configured max samples
                     sampleList = ret
                     if len(sampleList) <= maxSamples:
                         processSampleBcp(sampleList, updateExpKey)
                     else:
                        expMaxSamplesSet.add('Experiment in DB: %s' % expID) 

            # -- end "if expID in geoExptInDbDict:" ------------------------------

            typeList = list(map(str.strip, gdsType.split(';')))
            # 'Other' is only used if it is the only term in the typeList
            if 'Other' in typeList and len(typeList) > 1:
                skip = 1

            if skip != 1:
                (exptTypeKey, exptType) = processExperimentType(typeList)
                if exptTypeKey == 0:
                    # expts whose type doesn't translate and is not already in the db
                    expSkippedNotInDbNoTransSet.add(expID)
                    skip = 1

            if skip != 1 and isSuperSeries == 'yes':
                # number of superseries not already caught because of un translated
                # exptType or already in DB
                expSkippedNotInDbTransIsSuperseriesSet.add(expID)
                skip = 1
            # wts2-1339: expt not in db, we want to create the expt 
            # then later check how many samples
            if  skip != 1: 
                exptLoadedCount += 1
                createExpObject = 0
                # now process the samples
                ret =  processSamples(expID, 'false')
                if ret == 1:
                    expLoadedNoSampleList.append('expID: %s' % (expID))
                    createExpObject = 1
                elif ret == 2:
                    expSkippedNoSampleList.append('expID: %s' % (expID))
                    exptLoadedCount -= 1 # decrement the loaded count
                else:
                    sampleList = ret #  list of sampleString's representing each sample for the current experiment
                    createExpObject = 1
                if createExpObject:
                    # catenate the global overallDesign parsed from the sample to the
                    # experiment summary
                    description = '%s %s' % (summary, overallDesign)
                    description = removeNonAscii(description)
                    if runParsingReports == 'true':
                       fpExpParsingFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (expID, TAB, ', '.join(sampleList), TAB, title, TAB, description, TAB, isSuperSeries, TAB, pdat, TAB, exptType, TAB, ', '.join(pubmedList), CRT) )

                    #
                    # GXD_HTExperiment BCP
                    #

                    line = '%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextExptKey, TAB, sourceKey, TAB, title, TAB, description, TAB, pdat, TAB, releasedate, TAB, evalDate, TAB, evalStateTermKey, TAB, curStateTermKey, TAB, studyTypeTermKey, TAB, exptTypeKey, TAB, evalByKey, TAB, initCurByKey, TAB, lastCurByKey, TAB, initCurDate, TAB, lastCurDate, TAB, confidence, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT) 
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

                    #
                    # GXD_HTRawSample and MGI_KeyValue BCP
                    #
                    # ret from processSample = 1 means there was no sample file
                    # so experiment is created, but no samples wts2-1339 
                        # we've created the experiment, 
                        #   now check the number of samples
                    if DEBUG == 'true':
                        print('ret: %s len(sampleList): %s ' % (ret, len(sampleList)))
                    if ret == 1: #no sample file
                        pass # do nothing
                    elif len(sampleList) <= maxSamples:
                        processSampleBcp(sampleList, nextExptKey)
                    else: #ret != 1 and len(sampleList) > maxSamples
                        expMaxSamplesSet.add('New experiment: %s ' % expID)
                        
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
            if DEBUG == 'true':
                print('process experiment In tag level 4')
            # Accession tag at level 4 tells us we have a new record
            if elem.tag == 'Accession':
                expID = elem.text
            elif elem.tag == 'title':
                title = elem.text
                title = removeNonAscii(title)
            elif elem.tag == 'summary':    
                summary = elem.text
                if summary.find(SUPERSERIES) != -1:
                    isSuperSeries =  'yes'
            elif elem.tag == 'PDAT':
                pdat = elem.text
            elif elem.tag == 'gdsType':
                gdsType = elem.text
            elif elem.tag == 'n_samples':
                n_samples = elem.text
        if event=='start':
            level += 1
        elif elem.tag == 'int':
            id = elem.text
            pubmedList.append(id)
        elif level == 6 and elem.tag == 'Accession':
            sampleList.append(elem.text)
        if event == 'end':
            level -= 1
    
    elem.clear()
    return 0

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

def processSamples(expID, inDb): # inDb 'true' or 'false'
    global overallDesign, duplicatedSampleIdDict
    sampleFile = '%s%s' % (expID, sampleFileSuffix)
    samplePath = '%s/%s' % (geoDownloads, sampleFile)
    
    # if sample file does not exist return 1
    if not os.path.exists(samplePath):
        return 1

    # list of samples for this experiment to return
    sampleList = []

    # save the sample IDs for this experiment so we can check for dups
    idList = []

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
                channelList.append(channelDict)

            # process the 1 or 2 channels for the current sample
            channelString = processChannels(channelList)

            # the string of attributes representing the current sample
            sampleString = ('%s%s%s%s%s%s%s%s%s%s%s' % (expID, TAB, sampleID, TAB, description, TAB, title, TAB, sType, TAB, channelString))

            # append to the list of attributes for each sample in this 
            # experiment
            sampleList.append(sampleString)

            # optionally write this report for new experiments
            if runParsingReports == 'true' and inDb == 'false':
                fpSampParsingFile.write('%s%s' % (sampleString, CRT))

            # always write this report for experiments in the db
            if inDb == 'true':
                fpSampInDbParsingFile.write('%s%s' % (sampleString, CRT))

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
            if DEBUG == 'true':
                print('processSample tag level 2')
            if elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Sample':
                sampleID = str.strip(elem.get('iid'))
                if sampleID in idList:
                    if expID not in duplicatedSampleIdDict:
                        duplicatedSampleIdDict[expID] = []
                    duplicatedSampleIdDict[expID].append(sampleID)
                    continue
                idList.append(sampleID)
            elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Overall-Design':
                if overallDesign == None:
                    overallDesign = ''
                else:
                    #overallDesign = ((str.strip(elem.text)).replace(TAB, ' ')).replace(CRT, ' ')
                    overallDesign = elem.text
                    overallDesign = removeNonAscii(overallDesign)


        #
        # Tag Level 3
        #

        if level == 3:
            if DEBUG == 'true':
                print('processSample tag level 3')
            if elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Description':
                description = elem.text
                if description == None:
                    description = ''
                else:
                    description = description.replace('\\', '')
                    description =  removeNonAscii(description)

            elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Title':
                title = elem.text
                if title == None:
                    title = ''
                else:
                    title = removeNonAscii(title)
                    
            elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Type':
                sType = elem.text
                if sType == None:
                    sType = ''
                else:
                    sType = ((str.strip(sType)).replace(TAB, ' ')).replace(CRT, ' ')
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
                tag = ((str.strip(tag)).replace(TAB, ' ')).replace(CRT, ' ') 
                value = ((str.strip(elem.text)).replace(TAB, ' ')).replace(CRT, ' ') 
                
                #    (expID, sampleID, tag, value))
                if value is not None and value != '':
                    channelDict[tag] = value


        #
        # Tag Level 4
        #

        if level == 4:
            if DEBUG == 'true':
                print('processSample tag level 4')
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
                
                if treatmentProt is not None and treatmentProt != '':
                    treatmentProt = ((str.strip(treatmentProt)).replace(TAB, ' ')).replace(CRT, ' ')
                    if DEBUG == 'true':
                        print('adding to channelDict expID: %s sampleID: %s treatmentProt: %s' % (expID, sampleID, treatmentProt))
                    channelDict['treatmentProt'] = treatmentProt
            elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Molecule':
                molecule = elem.text
                if molecule is not None and molecule != '':
                        channelDict['molecule'] = str.strip(molecule)

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
    global curSampleGainLossList, ncSampleGainLossList
    global lostCt, gainedCt, lostSampleDict, gainedSampleDict

    #
    # sampleString looks like:
    # (expID, TAB, sampleID, TAB, description, TAB, title, TAB, sType, 
    #    TAB, channelString)
    #
    # channelString looks like:
    # '!!!' delim key:value,  '|||' delim channel
    #
    inputSampleIdSet = set()
    expID = ''

    # write experiment to be deleted
    fpSampleDelete.write(deleteTemplate % nextExptKey)

    for sampleString in sampleList:
        sampleLoadedCount += 1
        if DEBUG == 'true':
            print('sampleString: %s' % sampleString)

        expID, sampleID, description, title, sType, channelString = str.split(sampleString, TAB) 

        inputSampleIdSet.add(sampleID)

        # write to fpSampleBcp here
        fpSampleBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextRawSampleKey, TAB, nextExptKey, TAB, sampleID, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT))

        channels = str.split(channelString, '|||')
        seqNum = 1 # there can be 1 or 2 channels, data for each channel
                   # distinguished by seqNum

        # write out key/value for description, title and sType
        if description != None and description != '':
            fpKeyValueBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextKeyValueKey, TAB, nextRawSampleKey, TAB, rawSampleMgiTypeKey, TAB, 'description', TAB, description, TAB, seqNum, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT))
            nextKeyValueKey += 1
        if title != None and title != '':
            fpKeyValueBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextKeyValueKey, TAB, nextRawSampleKey, TAB, rawSampleMgiTypeKey, TAB, 'title', TAB, title, TAB, seqNum, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT))
            nextKeyValueKey += 1
        if sType != None and sType != '':
            fpKeyValueBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextKeyValueKey, TAB, nextRawSampleKey, TAB, rawSampleMgiTypeKey, TAB, 'sType', TAB, sType, TAB, seqNum, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT))
            nextKeyValueKey += 1

        for channel in channels:
            tokens = str.split(channel, '!!!')
            
            for keyValue in tokens:
                key, value = str.split(keyValue, ':::')
                value = value.replace('\\', r'\\\\')
                value = value.replace('#', r'\#')
                value = value.replace('?', r'\?')
                value = value.replace('\n', r'\\n')
                key = key.replace('\\', r'\\\\')
                key = key.replace('#', r'\#')
                key = key.replace('?', r'\?')
                key = key.replace('\n', r'\\n')

                # replace em-dash(unicode) with two en-dash
                value = value.replace(b'\xe2\x80\x94'.decode('utf-8'), '--')
                key = key.replace(b'\xe2\x80\x94'.decode('utf-8'), '--')

                # this removes non-ascii characters; 
                # 'replace' replaces with '?'
                value_encode = value.encode('ascii', 'replace')
                key_encode = key.encode('ascii', 'replace')

                value_decode = value_encode.decode()
                key_decode = key_encode.decode()

                if value_decode == '-' or value_decode == '' or value_decode is None:
                    value_decode = '--'
                
                # write to fpKeyValueBcp here
                fpKeyValueBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextKeyValueKey, TAB, nextRawSampleKey, TAB, rawSampleMgiTypeKey, TAB, key_decode, TAB, value_decode, TAB, seqNum, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT))
                nextKeyValueKey += 1     
            # increment, there may be a second channel
            seqNum += 1 

        # increment the sample key, multiple samples/experiment
        nextRawSampleKey += 1

    if expID in curatedExptDict:

        dbSampleIdSet = set(curatedExptDict[expID])

        # input rs not in database
        gainedSet = inputSampleIdSet.difference(dbSampleIdSet)

        # for debugging
        if expID not in gainedSampleDict:
            gainedSampleDict[expID] = set()
        gainedSampleDict[expID] = gainedSampleDict[expID].union(gainedSet)

        # db rs not in input
        lostSet = dbSampleIdSet.difference(inputSampleIdSet)

        # for debugging
        if expID not in lostSampleDict:
            lostSampleDict[expID] = set()
        lostSampleDict[expID] = lostSampleDict[expID].union(lostSet)

        if lostSet or gainedSet:
           lostCt += len(lostSet)
           gainedCt += len(gainedSet)
           gainString = ', '.join(str(s) for s in gainedSet)
           lostString = ', '.join(str(s) for s in lostSet)
           line = '%s%s%s%s%s' % (expID, TAB, gainString, TAB, lostString) 
           curSampleGainLossList.append(line)
    else:
        if expID in nonCuratedExptDict:
            
            dbSampleIdSet = set(nonCuratedExptDict[expID])

            # input rs not in database
            gainedSet = inputSampleIdSet.difference(dbSampleIdSet)
            
            # for debugging
            if expID not in gainedSampleDict:
                gainedSampleDict[expID] = set()
            gainedSampleDict[expID] = gainedSampleDict[expID].union(gainedSet)

            # db rs not in input
            lostSet = dbSampleIdSet.difference(inputSampleIdSet)

            # for debugging
            if expID not in lostSampleDict:
                lostSampleDict[expID] = set()
            lostSampleDict[expID] = lostSampleDict[expID].union(lostSet)

            if lostSet or gainedSet:
               lostCt += len(lostSet)
               gainedCt += len(gainedSet) 
               gainString = ', '.join(str(s) for s in gainedSet)
               lostString = ', '.join(str(s) for s in lostSet)
               if lostString == 'None':
                   lostString = 'Experiment had no samples prior'
                   lostCt -= 1 # deduct 1 as 'None' was counted above
               line = '%s%s%s%s%s' % (expID, TAB, gainString, TAB, lostString)
               
               ncSampleGainLossList.append(line)
            
    return 0

#
# Purpose: writes out QC to the QC file
# Returns: 
# Assumes: Nothing
# Effects:
# Throws: Nothing
#

def writeQC():
    # the curated experiment report    
    fpCuratedQcFile.write('* Number of curated experiments with updated samples (gains/losses):%s%s%s' % \
         (len(curSampleGainLossList), CRT, CRT))
    gainLossString = CRT.join(curSampleGainLossList)
    fpCuratedQcFile.write('%s%s%s' % (gainLossString, CRT, CRT))

    if DEBUG == 'true':
        # for debugging
        fpCuratedQcFile.write('* Number of non-curated experiments with updated samples (gains/losses):%s%s%s' % \
             (len(ncSampleGainLossList), CRT, CRT))
        gainLossString = CRT.join(ncSampleGainLossList)
        fpCuratedQcFile.write('%s%s%s' % (gainLossString, CRT, CRT))

        fpCuratedQcFile.write('* Net gain/loss of samples curated and non-curated gainedCt %s - lostCt %s: %s%s%s' % (gainedCt, lostCt, gainedCt - lostCt, CRT, CRT))

        # for debugging
        fpCuratedQcFile.write('* Gained samples for experiments in the database curated and non-curated: %s%s%s' % (gainedCt, CRT, CRT))
        for eId in gainedSampleDict:
            sSet = gainedSampleDict[eId]
            for sId in sSet:
                fpCuratedQcFile.write('   %s:%s%s' %  (eId, sId, CRT))

        # for debugging
        fpCuratedQcFile.write('* Lost samples for experiments in the database curated and non-curated: %s%s%s ' % (lostCt, CRT, CRT))
        for eId in lostSampleDict:
            sSet = lostSampleDict[eId]
            for sId in sSet:
                fpCuratedQcFile.write('   %s:%s%s' %  (eId, sId, CRT))

    # the load report
    fpQcFile.write('GEO HT Raw Data Load QC%s%s%s' % (CRT, CRT, CRT))

    fpQcFile.write('* Number of experiments in the input: %s%s%s' % \
        (expCount, CRT, CRT))

    fpQcFile.write('* Number of experiments loaded: %s%s%s' % \
        (exptLoadedCount, CRT, CRT))

    fpQcFile.write('* Number experiments, already in DB: %s%s%s' % \
        (len(geoExptInDbDict), CRT, CRT))

    fpQcFile.write('* Number of raw samples loaded: %s%s%s' % \
        (sampleLoadedCount, CRT, CRT))

    fpQcFile.write('* Number experiments that have > max samples, samples not loaded: %s%s%s' % \
        (len(expMaxSamplesSet), CRT, CRT))
    for id in expMaxSamplesSet:
        fpQcFile.write('    %s%s' %  (id, CRT))

    fpQcFile.write('* Number experiments skipped, not already in db. Type not in translation: %s%s%s' % \
        (len(expSkippedNotInDbNoTransSet), CRT, CRT))

    fpQcFile.write('* Number experiments skipped, not already in db. Type not in translation. Is SuperSeries: %s%s%s' % \
        (len(expSkippedNotInDbTransIsSuperseriesSet), CRT, CRT))

    fpQcFile.write('* Number experiments skipped because of Sample parsing issues: %s%s%s' % (len(expSkippedNoSampleList), CRT, CRT))
    for e in expSkippedNoSampleList:
        fpQcFile.write('    %s%s' %  (e, CRT))   

    fpQcFile.write('* Number experiments loaded w/o samples, no sample file: %s%s%s' % (len(expLoadedNoSampleList), CRT, CRT))
    for e in expLoadedNoSampleList:
        fpQcFile.write('    %s%s' %  (e, CRT))

    fpQcFile.write('* Number experiments with updated PubMed ID properties: %s%s%s' % (len(updateExptList), CRT, CRT))

    fpQcFile.write('* Number experiments with duplicated sample IDs: %s%s%s' % (len(duplicatedSampleIdDict), CRT, CRT))
    for eId in duplicatedSampleIdDict:
        sIds = ', '.join(duplicatedSampleIdDict[eId])
        fpQcFile.write('%s: %s%s' %  (eId, sIds, CRT))

    fpQcFile.write('* Set of unique GEO Experiment Types not found in Translation: %s%s%s' % (len(expTypesSkippedSet), CRT, CRT))
    sortedSet = sorted(expTypesSkippedSet)
    for type in sortedSet:
        fpQcFile.write('    %s%s' %  (type, CRT))

    return 0

# Purpose: Close file descriptors
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: Closes files
# Throws: Nothing
#
def closeFiles():

    fpQcFile.close()
    fpCuratedQcFile.close()
    fpExpParsingFile.close()
    fpSampParsingFile.close()
    fpSampInDbParsingFile.close()
    fpExperimentBcp.close()
    fpSampleBcp.close()
    fpKeyValueBcp.close()
    fpAccBcp.close()
    fpVariableBcp.close()
    fpPropertyBcp.close()

    fpSampleDelete.close()

    return 0

#
# main
#

if initialize() != 0:
    print("geo_htload failed initializing")
    sys.exit(1)

if processAll() != 0:
    print("geo_htload failed during processing")
    closeFiles()
    sys.exit(1)

if writeQC() != 0:
    print("geo_htload failed writing QC")
    closeFiles()
    sys.exit(1)

if closeFiles() != 0:
    print("geo_htload failed closing files")
    sys.exit(1)
