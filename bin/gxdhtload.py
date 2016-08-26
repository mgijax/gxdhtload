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

# ArrayExpress file and descriptor
inFileName = os.environ['INFILE_NAME']
fpInFile = None

# QC file and descriptor
qcFileName = os.environ['QCFILE_NAME']
fpQcFile = None

# Number or experiments in AE json file
expCount = 0

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

# data lookups

# all properties. key=primaryID, value=dictionary where key=controlled property term, value=list of raw values
propertiesDict = {}

# database lookups
#
# Purpose:  Open file descriptors. Create lookups
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: Copies & opens files, read a database
# Throws: Nothing
#
def initialize():
    global fpInFile, fpQcFile, jFile

    # create file descriptors
    try:
	fpInFile = open(inFileName, 'r')
    except:
	print '%s does not exist' % inFileName
    try:
	fpQcFile = open(qcFileName, 'w')
    except:
	 print '%s does not exist' % qcFileName

    jFile = json.load(fpInFile)

    db.useOneConnection(1)
    
    # load lookups

    db.useOneConnection(0)
    
    return
#
# Purpose: parse input file, QC, create bcp files
# Returns: 1 if file can be read/processed correctly, else 0
# Assumes: Nothing
# Effects: Creates files in the file system
# Throws: Nothing
#
def process():
    global propertiesDict, expCount, invalidSampleCountDict
    global invalidReleaseDateDict, invalidUpdateDateDict, noIdList
    
    for f in jFile['experiments']['experiment']:
        expCount += 1

	try:
            description = f['description'] # experiment, one
	except:
    	    description = ''
	try:
	    name = f['name'] # experiment and property, many; |-delim in both
	    if type(name) == types.ListType:
		name = '|'.join(name)
	except:
	    name = ''
	try:
	    organism = f['organism'] # property, many stored individually
	    if type(organism)  == types.ListType:
		organism = '|'.join(organism)
	except:
	    organism = ''
	try:
	    primaryID = f['accession'] # accession
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
            experimentdesign = f['experimentdesign'] # property, many stored ind
        except:
            experimentdesign = ''
        try:
	    # experimentalfactor.name
            experimentalfactor = f['experimentalfactor'] # property, 
							 # many stored individ.
        except:
            experimentalfactor = ''
        try:
            lastupdatedate = f['lastupdatedate'] # experiment, one
        except:
            lastupdatedate = ''
        try:
	    # provider.contact, need to remove exact dups
            provider = f['provider'] # property, many stored individually
        except:
            provider = ''
	# currently no secondary's needed from file
        #try:
        #    secondaryaccession = f['secondaryaccession'] 
        #except:
        #    secondaryaccession = ''
        try:
            experimenttype = f['experimenttype'] # property, many stored individ
        except:
            experimenttype = ''
        try:
	    # arraydesign.name
            arraydesign = f['arraydesign'] # property, many pipe delimited
        except:
            arraydesign = ''
        try:
	    # bibliography.accession
            bibliography = f['bibliography'] # propery, many stored individually
        except:
            bibliography = ''

	# Do QC checks
	if primaryID == '':
	    noIdList.append('Name: %s' % name)
	else:
	    checkPrimaryId(primaryID)

	# check that sample is integer
	# 8/26 samples not currently data attribute
	if sampleCount and checkInteger(sampleCount)== 0:
	    invalidSampleCountDict[primaryID] = sampleCount

	# check dates
        #print 'check releasedate'
        if releasedate != '' and checkDate(releasedate) == 0:
	    #print 'adding %s to invalidReleaseDateDict' % releasedate
	    invalidReleaseDateDict[primaryID] = releasedate

	#print 'check lastupdatedate'
	if lastupdatedate != '' and checkDate(lastupdatedate) == 0:
	    #print 'adding %s to invalidUpdateDateDict' % lastupdatedate
	    invalidUpdateDateDict[primaryID]= lastupdatedate
	
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
    # yyyy-mm-dd format
    ymd = re.compile('([0-9]{4})-([0-9]{2})-([0-9]{2})')
    ymdMatch = ymd.match(rawText)
    #print 'checkdate: %s' % rawText
    if ymdMatch:
	(year, month, day) = ymdMatch.groups()
	if (1950 <= int(year) <= 2050):
	    if (1 <= int(month) <= 12):
		if (1 <= int(day) <= 31):
		    #print 'return 1'
		    return 1
    #print 'return 0'
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
    fpQcFile.close()

    return
#
# main
#

initialize()
process()
writeQC()
closeFiles()

