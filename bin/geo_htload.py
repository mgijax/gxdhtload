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
import xml.etree.cElementTree as ET

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

# ArrayExpress LogicalDB key
aeLdbKey = 189

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
defaultEvalStateTermKey = 20225941 

# When SUPERSERIES appears in description
# 'No' from  'GXD HT Evaluation State' (vocab key = 116)
altEvalStateTermKey=20225943

# 'Not Done' from GXD HT Curation State' (vocab key=117)
curStateTermKey = 20475422

# When SUPERSERIES appears in description
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

# to form the sample file to process
geoDownloads = os.environ['GEO_DOWNLOADS']
sampleTemplate = os.environ['GEO_SAMPLE_TEMPLATE']

# QC file and descriptor
qcFileName = os.environ['QCFILE_NAME']
fpQcFile = None

#
# BCP files
#
experimentFileName = os.environ['EXPERIMENT_BCP']
fpExperimentBcp = None

accFileName = os.environ['ACC_BCP']
fpAccBcp = None

variableFileName = os.environ['VARIABLE_BCP']
fpVariableBcp =  None

propertyFileName = os.environ['PROPERTY_BCP']
fpPropertyBcp = None

# for MGI_Property:
expTypePropKey = 20475425
expFactorPropKey = 20475423
sampleCountPropKey = 20475424
contactNamePropKey = 20475426
namePropKey = 20475428
pubmedPropKey =	20475430 
propTypeKey = 1002

# Number of experiments in GEO xml files
expCount = 0

# Number experiments loaded
# starts with 1 because we count after we come to the next record
loadedCount = 1

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

# experiment types skipped because not in translation
expTypesSkippedSet = set()

# experments in the database skipped
expIdsInDbSet = set()

#
# Purpose:  Open file descriptors, get next primary keys, create lookups
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: Copies & opens files, read a database
# Throws: Nothing
#
def initialize():
    global fpQcFile, fpExperimentBcp, fpAccBcp, fpVariableBcp 
    global fpPropertyBcp, jFile, nextExptKey, nextAccKey, nextExptVarKey
    global nextPropKey

    # create file descriptors
    try:
        fpQcFile = open(qcFileName, 'w')
    except:
         print('Cannot create %s' % qcFileName)

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
    print('expID%ssampleList%stitle%ssummary%sisSuperSeries%spdat%sChosen Expt Type%sn_samples%spubmedList%s' % (TAB, TAB, TAB, TAB, TAB, TAB, TAB, TAB, TAB))
    #first = 1
    for expFile in str.split(os.environ['EXP_FILES']):
        #print(expFile)
        #if first == 1:
            process(expFile)
            #first = 0
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
    global nextExptKey, nextAccKey, nextExptVarKey, nextPropKey
    global updateExptCount, expTypesSkippedSet, expIdsInDbSet

    # scratch;
    #tree = ET.parse(expFile)
    #root = tree.getroot()
    #for child in root:
    #    print(child.tag, child.attrib)
    # DocumentSummarySet {'status': 'OK'}
    #for exp in root.iter('DocumentSummary'):
    #    print(exp.attrib)
    # {'uid': '200014873'}
    #event,root = context.next() next gives an error
    context = ET.iterparse(expFile, events=("start","end"))
    context = iter(context)
    
    level = 0
    expID = ''
    title = ''
    summary = ''
    isSuperSeries = 'no'
    pdat = ''
    gdsType = ''
    type = ''
    n_samples = ''
    pubmedList = []
    sampleList = [] # list of samplIDs
    exptTypeKey = 0
    inDb = 0
    for event, elem in context:
        if level == 4 : 
            # Accession tag at level 4 tells us we have a new record
            if elem.tag == 'Accession':
                expCount += 1
                # pick first valid experiment type and translate it 
                typeList = list(map(str.strip, gdsType.split(';')))
                #if expID == 'GSE154092':
                #print('typeList: %s' % typeList)
                for type in typeList:
                    #if expID == 'GSE154092':
                    #print ('type: %s' % type)
                    #type = str.strip(type)
                    if type in exptTypeTransDict:
                        exptTypeKey= exptTypeTransDict[type]
                        #if expID == 'GSE154092':
                        #print('type: %s key: %s' % (type, exptTypeKey))
                        break
                if expID in primaryIdDict:
                    inDb = 1
                    expIdsInDbSet.add(expID)
                print('expID: %s' % expID)
                print('isSuperSeries: %s' % isSuperSeries)
                print('title: %s' % (title))
                print('summary: %s' % summary)
                print('gdsType: %s' % gdsType)
                print('type: %s key: %s' % (type, exptTypeKey))
                print('pdat: %s' % pdat)
                print('n_samples: %s' % n_samples)
                print('sampleList: %s' % sampleList)
                print('pubmedList: %s' % pubmedList)

                if exptTypeKey != 0 and isSuperSeries == 'no' and inDb == 0:
                    # other wise print the row and reset the attributes
                    loadedCount += 1
                    print ('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (expID, TAB, ', '.join(sampleList), TAB, title, TAB, summary, TAB, isSuperSeries, TAB, pdat, TAB, type, TAB, n_samples, TAB, ', '.join(pubmedList)) )
                    processSamples(sampleList)
                else:
                     expTypesSkippedSet.update(typeList)
                expID = elem.text
                #if expID == 'GSE154092':
                #print('expID: %s' % expID)
                sampleList = []
                title = ''
                summary = ''
                isSuperSeries = 'no'  
                pdat = ''
                gdsType = ''
                n_samples = ''
                pubmedList = []
                exptTypeKey = 0
                inDb = 0
            elif elem.tag == 'title':
                title = elem.text
                #if expID == 'GSE154092':
                #print('title: %s' % (title))
            elif elem.tag == 'summary':    
                summary = elem.text
                #if expID == 'GSE154092':
                #print('summary: %s' % summary)
                if summary.find(SUPERSERIES) != -1:
                    isSuperSeries =  'yes'
            elif elem.tag == 'PDAT':
                pdat = elem.text
                #print('pdat: %s' % pdat)
            elif elem.tag == 'gdsType':
                gdsType = elem.text
                #if expID == 'GSE154092':
                #print('gdsType: %s' % gdsType)
            elif elem.tag == 'n_samples':
                n_samples = elem.text
                #if expID == 'GSE154092':
                #print('n_samples: %s' % n_samples)
        if event=='start':
            level += 1
            #print('level: %s elemTag: %s elemText: %s' % (level, elem.tag, elem.text))
        elif elem.tag == 'int':
            id = elem.text
            #print('id: %s' % id)
            #pubmedList.append(id)
        elif level == 6 and elem.tag == 'Accession':
            #if expID == 'GSE154092':
            #print('sampleID: %s' % elem.text)
            sampleList.append(elem.text)
        if event == 'end':
            level -= 1
        #if event == 'start' and level == 4 and elem.tag == 'Accession':
        #    print('event: %s level: %s elem: %s' % (event, level, elem.text))
    
    elem.clear()

def processSamples(sampleList):
    for id in sampleList:
        sampleFile = '%s%s' % (id, sampleTemplate)
        print('%s/%s' % (geoDownloads, sampleFile))

def oldprocess():
    global propertiesDict, expCount, loadedCount, inDbCount, invalidSampleCountDict
    global invalidReleaseDateDict, invalidUpdateDateDict, noIdList
    global nextExptKey, nextAccKey, nextExptVarKey, nextPropKey
    global updateExptCount

    for f in jFile['experiments']['experiment']:
        expCount += 1
        # definitions with SUPERSERIES text get different evaluation state 
        # than the load default and evalution date and evaluated by are set 
        # by the load (default null)
        isSuperSeries = 0
        evalStateToUseKey = defaultEvalStateTermKey

        try:
            # description is str.or list
            allDescription =  f['description']
            description = allDescription['text'] # experiment, onea
            
            # US108 'clean up URLs that appear in description field'. All
            # URLs that need to be cleaned up are the listType description
            # example of element in a description list with URL that we 
            # need to parse:
            # {'a': {'href': 'http://lgsun.grc.nia.nih.gov/ANOVA/', 'target': '_blank', '$': 'http://lgsun.grc.nia.nih.gov/ANOVA/'}}
            if type(description) ==  list:
                listDescript = ''
                for d in description:
                    if type(d) == dict: 
                        if 'a' in d:
                            url = d['a']['$']
                            listDescript = listDescript + url
                        # skip these: {"br":null}
                        elif 'br' in d:
                            continue
                    else:
                        listDescript = listDescript + str(d)
                description = listDescript
        except:
            description = ''
        if description == None: #  {'text': None, 'id': None}
            description = ''
        
        description = str.strip(description)

        if description.find(SUPERSERIES) != -1:
            evalStateToUseKey = altEvalStateTermKey
            isSuperSeries = 1

        try:
            name = f['name'] 
            if type(name) == list:
                name = '|'.join(name)
        except:
            name = ''
        name = str.strip(name)

        try:
            primaryID = str.strip(f['accession']) # accession
        except:
            primaryID = ''
        
        try:
            sampleCount = f['samples'] # property, one
        except:
            sampleCount = ''

        try:
            releasedate = f['releasedate'] # experiment, one
        except:
            releasedate = ''

        try:
            # experimentalfactor.name
            # list or dict
            expFactor = f['experimentalfactor']
            if type(expFactor) == dict:
                expFactorList = [expFactor]
            else:
                expFactorList = expFactor
            expFactorSet = set()
            for e in expFactorList :  #property, many stored individ. 
                                      # weed out dups
                expFactorSet.add(e['name'])
            expFactorList = list(expFactorSet)
        except:
            expFactorList = []

        try:
            lastupdatedate = f['lastupdatedate'] # experiment, one
        except:
            lastupdatedate = ''

        try:
            # provider.contact, dictionary or list of dictionaries; need 
            # to remove exact dups
            providerList = []
            if type(f['provider']) != list:
                providerList = [f['provider']['contact']]
            else:
                for p in f['provider']:
                     if p['contact'] != None:
                         providerList.append(p['contact'])
            providerSet = set(providerList)
            providerList = list(providerSet)
        except:
            providerList = []

        try:
            # experimenttype is str.or list, property, 
            # many stored individ
            if type( f['experimenttype']) != list:
                experimenttypeList = [ f['experimenttype']]
            else:
                experimenttypeList =  f['experimenttype']
        except:
            experimenttypeList = []

        # pick first valid experiment type and translate it to populate the
        # exptype key
        exptTypeKey = 0
        for exp in experimenttypeList:
            if exp in exptTypeTransDict:
                exptTypeKey= exptTypeTransDict[exp]
                break
        if exptTypeKey == 0:
             exptTypeKey = exptTypeNRKey # Not Resolved

        try:
        # PubMed IDs - bibliography.accession
        # TR13116/check for duplicate pubmedids
            bibliographyList = []
            if type(f['bibliography']) == dict: # dictionary
                 if str(f['bibliography']['accession']) not in bibliographyList:
                        bibliographyList.append(str(f['bibliography']['accession']))
            else: # ListType
                for b in f['bibliography']: # for each dict in the list
                    if 'accession' in b:
                        if str(b['accession']) not in bibliographyList:
                                bibliographyList.append(str(b['accession']))
        except:
            bibliographyList = []

        # the template for properties:
        propertyTemplate = "#====#%s%s%s#=#%s%s%s%s%s#==#%s#===#%s%s%s%s%s%s%s%s%s" % (TAB, propTypeKey, TAB, TAB, nextExptKey, TAB, mgiTypeKey, TAB, TAB, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT )
        propertyUpdateTemplate = "#====#%s%s%s#=#%s#=====#%s%s%s#==#%s#===#%s%s%s%s%s%s%s%s%s" % (TAB, propTypeKey, TAB, TAB, TAB, mgiTypeKey, TAB, TAB, TAB, userKey, TAB, userKey, TAB, loadDate, TAB, loadDate, CRT )
        # 
        # update pubmed ID properties, if this ID already in the database
        #
        if primaryID in primaryIdDict:
            inDbCount += 1

            # not all experiments have pubmed IDs
            if primaryID in pubMedByExptDict:
                # get the list of pubmed Ids for this expt in the database
                dbBibList = pubMedByExptDict[primaryID]

                # get the set of incoming pubmed IDs not in the database
                newSet = set(bibliographyList).difference(set(dbBibList))

                # if we have new pubmed IDs, add them to the database
                if newSet:
                    updateExpKey = primaryIdDict[primaryID]

                    # get next sequenceNum for this expt's pubmed ID 
                    # in the database
                    results = db.sql('''select max(sequenceNum) + 1 
                        as nextNum
                        from MGI_Property p
                        where p._Object_key =  %s
                        and p._PropertyTerm_key = 20475430
                        and p._PropertyType_key = 1002''' % updateExpKey, 'auto')

                    nextSeqNum = results[0]['nextNum']
                    if newSet:
                        updateExptCount += 1
                    for b in newSet:
                        toLoad = propertyUpdateTemplate.replace('#=#', str(pubmedPropKey)).replace('#==#', str(b)).replace('#===#', str(nextSeqNum)).replace('#====#', str(nextPropKey)).replace('#=====#', str(updateExpKey))
                        fpPropertyBcp.write(toLoad)
                        nextPropKey += 1
            # continue so we don't dup what is in the db
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

        # evaluated data is today
        if isSuperSeries:
            line = line + loadDate + TAB
        else:
            # evaluated_date is null
            line = line + TAB

        line = line + str(evalStateToUseKey) + TAB

        if isSuperSeries:
             line = line + str(altCurStateTermKey) + TAB
        else:
            line = line + str(curStateTermKey) + TAB

        line = line + str(studyTypeTermKey) + TAB
        line = line + str(exptTypeKey) + TAB

        # evalByKey  is null unless isSuperSeries is true then 
        # it is load user
        if isSuperSeries:
            line = line + str(userKey) + TAB
        else:
            line = line + TAB

        # initialCurByKey, lastCurByKey, initialCurDate, lastCurDate 
        # all null
        line = line + TAB + TAB + TAB + TAB

        # created and modified by
        line = line + str(userKey) + TAB + str(userKey) + TAB

        # creation and modification date
        line = line + loadDate + TAB + loadDate + CRT

        fpExperimentBcp.write(line)

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

        #
        # Properties 
        #
        
        # name (0,1, pipe-delim) 
        # sampleCount (0,1) 
        # expFactorList (0-n)
        # providerList (0-n)
        # experimenttypeList (0-n)
        # bibiliographyList (0-n)
        #
        # propName, value and sequenceNum to be filled in later
        if name != '':
            toLoad = propertyTemplate.replace('#=#', str(namePropKey)).replace('#==#', name).replace('#===#', '1').replace('#====#', str(nextPropKey))
            fpPropertyBcp.write(toLoad)
            nextPropKey += 1

        if sampleCount != '':
            toLoad = propertyTemplate.replace('#=#', str(sampleCountPropKey)).replace('#==#', str(sampleCount)).replace('#===#', '1').replace('#====#', str(nextPropKey))
            fpPropertyBcp.write(toLoad)
            nextPropKey += 1

        seqNumCt = 1
        for e in expFactorList:
            toLoad = propertyTemplate.replace('#=#', str(expFactorPropKey)).replace('#==#', e).replace('#===#', str(seqNumCt)).replace('#====#', str(nextPropKey))
            fpPropertyBcp.write(toLoad)
            seqNumCt += 1
            nextPropKey += 1

        seqNumCt = 1
        for p in providerList:
            toLoad = propertyTemplate.replace('#=#', str(contactNamePropKey)).replace('#==#', p).replace('#===#', str(seqNumCt)).replace('#====#', str(nextPropKey))
            fpPropertyBcp.write(toLoad)
            seqNumCt += 1
            nextPropKey += 1

        seqNumCt = 1
        for e in experimenttypeList:
            toLoad = propertyTemplate.replace('#=#', str(expTypePropKey)).replace('#==#', e).replace('#===#', str(seqNumCt)).replace('#====#', str(nextPropKey))
            fpPropertyBcp.write(toLoad)
            seqNumCt += 1
            nextPropKey += 1

        seqNumCt = 1
        for b in bibliographyList:
            toLoad = propertyTemplate.replace('#=#', str(pubmedPropKey)).replace('#==#', str(b)).replace('#===#', str(seqNumCt)).replace('#====#', str(nextPropKey))
            fpPropertyBcp.write(toLoad)
            seqNumCt += 1
            nextPropKey += 1

        nextExptKey += 1

    return

def writeQC():
    global expTypesSkippedSet, expIdsInDbSet

    fpQcFile.write('GEO HT Raw Data Load QC%s%s%s' % (CRT, CRT, CRT))

    fpQcFile.write('Number of experiments in the input: %s%s%s' % \
        (expCount, CRT, CRT))
    fpQcFile.write('Number of experiments loaded: %s%s%s' % \
        (loadedCount, CRT, CRT))

    fpQcFile.write('GEO Experiment Types Skipped because not in Translation: %s' % (len(expTypesSkippedSet)))
    expTypesSkippedSet = sorted(expTypesSkippedSet)
    for type in expTypesSkippedSet:
        fpQcFile.write('    %s%s' %  (type, CRT))

    fpQcFile.write('%sExperiments already in the database:%s%s' %\
         (CRT, len(expIdsInDbSet), CRT))
    expIdsInDbSet = sorted(expIdsInDbSet)
    for id in expIdsInDbSet:
        fpQcFile.write ('    %s%s' %  (id, CRT))


def oldwriteQC():
    fpQcFile.write('GXD HT Raw Data Load QC%s%s%s' % (CRT, CRT, CRT))

    fpQcFile.write('Number of experiments in the input: %s%s' % \
        (expCount, CRT))
    fpQcFile.write('Number of experiments already in the database: %s%s' %\
         (inDbCount, CRT))
    fpQcFile.write('Number of experiments loaded: %s%s' % \
        (loadedCount, CRT))
    fpQcFile.write('Number of experiments with updated PubMed IDs: %s%s%s'\
         % (updateExptCount, CRT, CRT))


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
processAll()
writeQC()
closeFiles()
