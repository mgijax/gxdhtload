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

# Array Express primary ID prefix for GEO experiments
AEGEOPREFIX = 'E-GEOD-'

# GEO primary ID prefix for experiments
GEOPREFIX = 'GSE'

SUPERSERIES='This SuperSeries'

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

# 'GEO' from 'GXD HT Source' (vocab key = 119)
sourceKey = 87145238

# no release or lastupdatedate
releasedate = ''
lastupdatedate = ''

#
# File Descriptors:
#

# load only experiments with <= MAX_SAMPLES
maxSamples = os.environ['MAX_SAMPLES']

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
runParsingReports = os.environ['RUN_PARSING_RPTS']

#
# For bcp 
#
# for bcp
bcpin = '%s/bin/bcpin.csh' % os.environ['PG_DBUTILS']
server = os.environ['MGD_DBSERVER']
database = os.environ['MGD_DBNAME']

expt_table = 'GXD_HTExperiment'
acc_table = 'ACC_Accession'
exptvar_table = 'GXD_HTExperimentVariable'
property_table = 'MGI_Property'
outputDir = os.environ['OUTPUTDIR']

experimentFileName = os.environ['EXPERIMENT_FILENAME']
fpExperimentBcp = None

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
loadedCount = 0

# Number of experiments in the db whose pubmed IDs were updated
updateExptCount = 0

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
# {primaryID:key, ...}
primaryIdDict = {}

# raw experiment types mapped to controlled vocabulary keys
exptTypeTransDict = {}

# pubmed properties by AE ID (primary) in the db
# {AE ID: [pubmedId1, ..., pubmedIdn], ...}
pubMedByExptDict = {}

# experiment ids in the database skipped, primary skip
expIdsInDbSet = set()

# experiment types skipped because not in translation
expTypesSkippedSet = set()

# experiment ids not in database, type not translated, secondary skip
expSkippedNotInDbNoTransSet = set()

# experiments skipped because of 'Third-party reanalysis'
tprSet = set()

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
# Effects: Copies & opens files, read a database
# Throws: Nothing
#
def initialize():
    global fpQcFile, fpExpParsingFile, fpSampParsingFile, fpExperimentBcp 
    global fpAccBcp, fpVariableBcp, fpPropertyBcp, jFile, nextExptKey
    global nextAccKey, nextExptVarKey, nextPropKey

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
    results = db.sql('''select a.accid, p.value
        from GXD_HTExperiment e, ACC_Accession a, MGI_Property p
        where e._Experiment_key = a._Object_key
        and a._MGIType_key = %s
        and a._LogicalDB_key = %s
        and a.preferred = 1
        and e._Experiment_key = p._Object_key
        and p._PropertyTerm_key = %s
        and p._PropertyType_key = %s''' % \
            (mgiTypeKey, geoLdbKey, pubmedPropKey, propTypeKey), 'auto')

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

#
# Purpose: calculate a GEO ID from and AE GEO ID
# Returns: GEO ID or empty str.if not an AE GEO ID
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
# Purpose: Loops through all metadata files sending them to parser
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
        #print(expFile[-1])
        #if expFile[-1] != '9': # files number 1 - 10, use 0 for file 10
        #    print ('skipping: %s' % expFile)
        #    continue
        process(expFile)
    return

#
# Purpose: parse input file, QC, create bcp files
# Returns: 1 if file can be read/processed correctly, else 0
# Assumes: Nothing
# Effects: Creates files in the file system
# Throws: Nothing
#

def process(expFile):
    global propertiesDict, expCount, loadedCount, invalidSampleCountDict
    global invalidReleaseDateDict, invalidUpdateDateDict, noIdList
    global nextExptKey, nextAccKey, nextExptVarKey, nextPropKey, expSkippedNotInDbTransIsSuperseriesSet
    global updateExptCount, expTypesSkippedSet, expIdsInDbSet
    global expSkippedNoSampleList, expLoadedNoSampleList, expSkippedNotInDbNoTransSet
    global overallDesign, tprSet, expSkippedMaxSamplesSet

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
            print('expID: %s' % expID)
            allExptIdList.append(expID)

            if expID in primaryIdDict:
                skip = 1
                expIdsInDbSet.add(expID)
                print('    expIdInDb skip')

            typeList = list(map(str.strip, gdsType.split(';')))
            if skip != 1 and 'Third-party reanalysis' in typeList:
                    tprSet.add(expID)
                    print("ExpID: %s is 'Third-party reanalysis'" % expID)
                    print('TypeList: %s' % typeList)
                    skip = 1
                    print('    tprSet.add skip')

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
            print('exptTypeKey: %s isSuperSeries: %s skip: %s' % (exptTypeKey, isSuperSeries, skip))
            if  skip != 1: 
                # print the row for testing purposes
                loadedCount += 1
                createExpObject = 0
                # now process the samples
                rc =  processSamples(expID, n_samples)
                if rc == 1:
                    expLoadedNoSampleList.append('expID: %s' % (expID))
                    createExpObject = 1
                elif rc == 2:
                    expSkippedNoSampleList.append('expID: %s' % (expID))
                    loadedCount -= 1 # decrement the loaded count
                else:
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

                    line = '%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextExptKey, TAB, sourceKey, TAB, title, TAB, description, TAB, releasedate, TAB, pdat, TAB, loadDate, TAB, evalStateTermKey, TAB, curStateTermKey, TAB, studyTypeTermKey, TAB, exptTypeKey, TAB, evalByKey, TAB, initCurByKey, TAB, lastCurByKey, TAB, initCurDate, TAB, lastCurDate, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT) 
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
                    fpAccBcp.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextAccKey, TAB, expID, TAB, prefixPart, TAB, numericPart, TAB, geoLdbKey, TAB, nextExptKey, TAB, mgiTypeKey, TAB, private, TAB, isPreferred, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT ))
                    nextAccKey += 1

                    #
                    # Properties
                    #

                    # title (1) experiment name namePropKey = 20475428
                    # n_samples (1) count of samples  sampleCountPropKey = 20475424
                    # typeList (1-n) raw experiment types expTypePropKey = 20475425
                    # pubmedList (0-n) pubmed Ids pubmedPropKey = 20475430
                    # description (1) sample overalldesign + expt summary descriptionPropKey = 87508020
                    # the template for properties:
                    propertyTemplate = "#====#%s%s%s#=#%s%s%s%s%s#==#%s#===#%s%s%s%s%s%s%s%s%s" % (TAB, propTypeKey, TAB, TAB, nextExptKey, TAB, mgiTypeKey, TAB, TAB, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT )


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

def processExperimentType(typeList):
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

def processSamples(expID, n_samples):
    global overallDesign

    #print('expID: %s' % (expID))
    sampleFile = '%s%s' % (expID, sampleFileSuffix)
    samplePath = '%s/%s' % (geoDownloads, sampleFile)
    #print(samplePath)
    # check that sample file exists
    if not os.path.exists(samplePath):
        #print('sample file does not exist: %s' % samplePath)
        return 1
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

    # Channel, there can be 1 or 2, need for sequence of sets of source/taxid/treatment/molecule
    cCount = 0 
    try:
        for event, elem in context:
            if event == 'start':
                level += 1
            if event == 'end':
                level -= 1
            # we are done processing a sample, print and reset
            if event == 'end' and elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Sample':
                # if the dict is not empty, add it to the list
                if channelDict:
                    #print(channelDict)
                    #print('set second channel in channelList')
                    channelList.append(channelDict)

                channelString = processChannels(channelList)
                if runParsingReports == 'true':
                    fpSampParsingFile.write('%s%s%s%s%s%s%s%s%s%s%s%s' % (expID, TAB, sampleID, TAB, description, TAB, title, TAB, sType, TAB, channelString, CRT))
                # reset
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

            if level == 2:
                if elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Sample':
                    sampleID = str.strip(elem.get('iid'))
                elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Overall-Design':
                    if overallDesign == None:
                        overallDesign = ''
                    else:
                        overallDesign = str.strip(elem.text)
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
                        title = str.strip(title)
                elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Type':
                    sType = elem.text
                    if sType == None:
                        sType = ''
                    else:
                        sType = str.strip(sType)
                elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Channel':
                    #print('sId: %s tag: %s text: %s attrib: %s' % (sampleID, elem.tag, elem.text, elem.get('position')))
                    cCount = int(elem.get('position'))
                    # if we have a second channel, append the first to the List and reset the dict
                    if cCount == 2:
                        channelList.append(channelDict)
                        #print('set first channel in channelList')
                        channelDict = {}
                elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Characteristics':
                    tag = elem.get('tag')
                    if tag == None: # not an attrib just get the text
                        tag = 'Characteristics' # name it
                    tag = str.strip(tag)   # strip it, might be attrib
                    value = str.strip(elem.text) # get the value
                    #print('expID: %s sampleID: %s tag: %s value: %s' % (expID, sampleID, tag, value))
                    if value != None and value != '':
                        channelDict[tag] = value
            
            if level == 4:
                if elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Source':
                    source = elem.text
                    if source != None:
                        channelDict['source'] = str.strip(source)
                elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Organism':
                    taxid = elem.get('taxid')
                    if taxid != None:
                        channelDict['taxid'] = str.strip(taxid)
                    taxidValue = elem.text
                    if taxidValue != None:
                        channelDict['taxidValue'] = str.strip(taxidValue)

                elif elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Treatment-Protocol':
                    treatmentProt = elem.text
                    if treatmentProt != None:
                        channelDict['treatmentProt'] = ((str.strip(treatmentProt)).replace(TAB, '')).replace(CRT, '')
                elif elem.tag == elem.tag == '{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Molecule':
                    molecule = elem.text
                    if molecule != None:
                        channelDict['molecule'] = str.strip(molecule)
    except:
        return 2

    return 0 
def processChannels(channelList):
    # no channels in this sample, return empty string
    if not channelList:
        return ''
    if len(channelList) == 1:
        return processOneChannel(channelList[0])
    else:
        string1 = processOneChannel(channelList[0])
        string2 = processOneChannel(channelList[1])
        return '%s|||%s' % (string1, string2)

def processOneChannel(channelDict):
    keyValueList = []
    for key in channelDict:
        keyValueList.append('%s:%s' % (key, channelDict[key]))
    return ', '.join(keyValueList)

def writeQC():
    global expTypesSkippedSet, expIdsInDbSet

    fpQcFile.write('GEO HT Raw Data Load QC%s%s%s' % (CRT, CRT, CRT))

    fpQcFile.write('Number of experiments in the input: %s%s%s' % \
        (expCount, CRT, CRT))

    fpQcFile.write('Number of experiments loaded: %s%s%s' % \
        (loadedCount, CRT, CRT))

    fpQcFile.write('Number experiments skipped, already in DB: %s%s' %\
        (len(expIdsInDbSet), CRT))
    for id in  expIdsInDbSet:
        fpQcFile.write('    %s%s' %  (id, CRT))

    fpQcFile.write('%sNumber experiments skipped, not already in db. Is Third-party reanalysis: %s%s' % \
        (CRT, len(tprSet),  CRT))
    for id in  tprSet:
        fpQcFile.write('    %s%s' %  (id, CRT))
       
    fpQcFile.write('%sNumber experiments skipped, not already in db, not Third-party reanalysis. Type not in translation: %s%s%s' % \
        (CRT, len(expSkippedNotInDbNoTransSet), CRT, CRT))
    for id in  expSkippedNotInDbNoTransSet:
        fpQcFile.write('    %s%s' %  (id, CRT))

    fpQcFile.write('%sNumber experiments skipped, not already in db, not Third-party reanalysis, type not in translation. Is SuperSeries: %s%s%s' % \
        (CRT, len(expSkippedNotInDbTransIsSuperseriesSet), CRT, CRT))
    for id in  expSkippedNotInDbTransIsSuperseriesSet:
        fpQcFile.write('    %s%s' %  (id, CRT))

    fpQcFile.write('%sNumber experiments skipped, not already in db, not Third-party reanalysis, type not in translation, is not SuperSeries, has > max samples: %s%s%s' % \
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
    expTypesSkippedSet = sorted(expTypesSkippedSet)
    for type in expTypesSkippedSet:
        fpQcFile.write('    %s%s' %  (type, CRT))

def doBCP():
    # Purpose: executes bcp
    # Returns: 1 if error, else 0
    # Assumes: 
    # Effects: executes bcp, writes to the database
    # Throws: Nothing
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
