
#  ExptQC.py
###########################################################################
#
#  Purpose:
#
#	This script will generate a QC report for ArrayExpress
#	    experiment input file
#
#  Usage:
#
#      exptQC.py  filename
#
#      where:
#          filename = path to the input file
#
#  Inputs:
#      - input file as parameter - see USAGE
#
#  Outputs:
#
#      - QC report (${QC_RPT})
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#      2:  
#
#  Assumes:
#
#  Implementation:
#
#      This script will perform following steps:
#
#      1) Validate the arguments to the script.
#      2) Perform initialization steps.
#      3) Open the input/output files.
#      4) Generate the QC reports.
#      6) Close the input/output files.
#
#  History:
#
# History:
#
# sc  01/12/2024
#       - created WTS2-545, FL2 project
#
###########################################################################

import sys
import os
import string
import db
import time
import Set

#
#  CONSTANTS
#
TAB = '\t'
CRT = '\n'

USAGE = 'Usage: exptQC.py  inputFile'

#
#  GLOBALS
#

# intermediate load ready file
#loadReadyFile = os.getenv("INPUT_FILE_QC")
#fpLoadReady = None

# from stdin
inputFile = None

# QC report file
qcRptFile = os.getenv('QC_RPT')

# get the configured experiment prefix(es)
exptPrefixes = os.getenv('EXPT_PREFIXES')

# create a list and query string from those prefix(es)
exptPrefixList = list(map(str.strip, str.split(exptPrefixes, ',')))
exptQueryString = ''
for e in exptPrefixList:
    exptQueryString = exptQueryString + "'%s," % e
length =  len(exptQueryString)
exptQueryString + exptQueryString[:length - 1]

cmd = '''select accid as exptID 
        from acc_Accession a, gxd_Experiment e
        where a._mgitype_key = 42
        and a._logicaldb_key = 190
        and a._object_key = e._experiment_key
        and e._source_key = 87145238 --GEO'''
print('cmd: %s' % cmd)

# lines seen in the input file
distinctLineList = []

# duplicated lines in the input
dupeLineList = []

# lines with < 6 columns
missingColumnList = []

# lines with missing data in columns
reqColumnList = []

# a QTL id is not found in the database
badExptIdList = []

# 1 if any QC errors in the input file
hasFatalErrors = 0


# Purpose: Validate the arguments to the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: sets global variable, exits if incorrect # of args
# Throws: Nothing
#
def checkArgs ():
    global inputFile

    if len(sys.argv) != 2:
        print(USAGE)
        sys.exit(1)

    inputFile = sys.argv[1]
    return 0

# end checkArgs() -------------------------------

# Purpose: create lookups, open files
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables, exits if a file can't be opened,
#  creates files in the file system, creates connection to a database

def init ():

    # open input/output files
    openFiles()

    # load lookups
    #cmd = ' START HERE CREATE QUERY USING exptQueryString
    db.useOneConnection(1)


    return 0

# end init() -------------------------------

#
# Purpose: Open input and output files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def openFiles ():
    global fpInput, fpQcRpt

    #
    # Open the input file
    #
    # encoding='utf-8' no
    # encoding=u'utf-8' no

    try:
        fpInput = open(inputFile, 'r', encoding='utf-8', errors='replace')
    except:
        print('Cannot open input file: %s' % inputFile)
        sys.exit(1)

    #
    # Open QC report file
    #
    try:
        fpQcRpt = open(qcRptFile, 'w')
    except:
        print('Cannot open report file: %s' % qcRptFile)
        sys.exit(1)

    return 0

# end openFiles() -------------------------------

#
# Purpose: writes out errors to the qc report
# Returns: Nothing
# Assumes: Nothing
# Effects: writes report to the file system
# Throws: Nothing
#

def writeReport():
    #
    # Now write any errors to the report
    #
    if not hasFatalErrors:
         fpQcRpt.write('No QC Errors')
         return 0
    fpQcRpt.write('Fatal QC - if published the file will not be loaded')

    if len(dupeLineList):
        fpQcRpt.write(CRT + CRT + str.center('Lines Duplicated In Input',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(dupeLineList))
        fpQcRpt.write(CRT + 'Total: %s' % len(dupeLineList))

    if len(missingColumnList):
        fpQcRpt.write(CRT + CRT + str.center('Lines with < 6 Columns',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(missingColumnList))
        fpQcRpt.write(CRT + 'Total: %s' % len(missingColumnList))

    if len(reqColumnList):
        hasSkipErrors = 1
        fpQcRpt.write(CRT + CRT + str.center('Missing Data in Required Columns',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(reqColumnList))
        fpQcRpt.write(CRT + 'Total: %s' % len(reqColumnList))

    if len(orgPartSameList):
        fpQcRpt.write(CRT + CRT + str.center('Organizer and Participant have same ID',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(orgPartSameList))
        fpQcRpt.write(CRT + 'Total: %s' % len(orgPartSameList))

    if len(badQtlIdList):
        fpQcRpt.write(CRT + CRT + str.center('Invalid Organizer and/or Participant ID',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(badQtlIdList))
        fpQcRpt.write(CRT + 'Total: %s' % len(badQtlIdList))

    if len(idSymDiscrepList):
        fpQcRpt.write(CRT + CRT + str.center('Organizer and/or Participant ID does not match Symbol',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(idSymDiscrepList))
        fpQcRpt.write(CRT + 'Total: %s' % len(idSymDiscrepList))

    if len(badIntTermList):
        fpQcRpt.write(CRT + CRT + str.center('Interaction Term does not Resolve',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(badIntTermList))
        fpQcRpt.write(CRT + 'Total: %s' % len(badIntTermList))

    if len(badJnumList):
        fpQcRpt.write(CRT + CRT + str.center('JNumber value is not in the Database',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(badJnumList))
        fpQcRpt.write(CRT + 'Total: %s' % len(badJnumList))

    if len(noReciprocalList):
        fpQcRpt.write(CRT + CRT + str.center('No Reciprocal for Organizer/Participant',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#', 'Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(noReciprocalList))
        fpQcRpt.write(CRT + 'Total: %s' % len(noReciprocalList))

    return 0

# end writeReport() -------------------------------

#
# Purpose: Close the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Modifies global variables
# Throws: Nothing
#
def closeFiles ():
    #global fpInput, fpLoadReady, fpQcRpt
    global fpInput, fpQcRpt
    fpInput.close()
    #fpLoadReady.close()
    fpQcRpt.close()

    return 0

# end closeFiles) -------------------------------

    #
    # Purpose: run all QC checks
    # Returns: Nothing
    # Assumes: file descriptors have been initialized
    # Effects: writes reports and the load ready file to file system
    # Throws: Nothing
    #

def runQcChecks():
    global hasFatalErrors, distinctLineList, dupeLineList, qtlPairDict
    global missingColumnList, reqColumnList, orgPartSameList, badQtlIdList
    global idSymDiscrepList, badIntTermList, noReciprocalList, badJnumList


    header = fpInput.readline()
    line = fpInput.readline()
    lineNum = 1
    while line:
        lineNum += 1
        #print('lineNum: %s line: %s' % (lineNum, line))
        if line not in distinctLineList:
            distinctLineList.append(line)
        else:
            dupeLineList.append('%s  %s' % (lineNum, line))
        # check that the file has at least 23 columns
        if len(str.split(line, TAB)) < 6:
            missingColumnList.append('%s  %s' % (lineNum, line))
            hasFatalErrors = 1
            line = fpInput.readline()
            continue
        # get columns 1-6 
        (orgID, orgSym, partID, partSym, interactionType, jNum) = list(map(str.strip, str.split(line, TAB)))[:6]

        # all columns required
        if orgID == '' or orgSym == '' or partID == '' or partSym == '' or interactionType == '' or jNum == '':
            reqColumnList.append('%s  %s' % (lineNum, line))
            hasFatalErrors = 1

        # add the qtl org and part to the qtlPairDict - later we will check for reciprocals
        key = '%s|%s' % (orgID, partID)
        if key not in qtlPairDict:
            qtlPairDict[key] = []
        qtlPairDict[key].append('%s %s' % (lineNum, line))

        # Now verify each column

        # are the organizer and participant different?
        if orgID == partID:
            orgPartSameList.append('%s  %s' % (lineNum, line))
            hasFatalErrors = 1
        # is orgID a qtl ID?
        if orgID not in qtlLookupDict:
            badQtlIdList.append('%s  %s' % (lineNum, line))
            hasFatalErrors = 1
        else:
            # does orgSym match orgID?
           if orgSym != qtlLookupDict[orgID]:
                idSymDiscrepList.append('%s  %s' % (lineNum, line))
                hasFatalErrors = 1
        # is partID  a qtl ID?
        if partID not in qtlLookupDict:
            badQtlIdList.append('%s  %s' % (lineNum, line))
            hasFatalErrors = 1
        else:
            # does partSym match partID?
           if partSym != qtlLookupDict[partID]:
                idSymDiscrepList.append('%s  %s' % (lineNum, line))
                hasFatalErrors = 1
        # is interactionType a real term?
        if interactionType not in interactionLookupList:
            badIntTermList.append('%s  %s' % (lineNum, line))
            hasFatalErrors = 1
        
        if jNum not in jNumLookupList:
            badJnumList.append('%s  %s' % (lineNum, line))
            hasFatalErrors = 1
        
        line = fpInput.readline()
    # now check for reciprocals
    #for key in qtlPairDict:
    #    print('%s: %s' % (key, qtlPairDict[key]))
    for pair in qtlPairDict:
        (org, part) =  str.split(pair, '|')
        reciprocal = '%s|%s' % (part, org)
        if reciprocal not in qtlPairDict:
            #print('reciprocal not found')
            pList = qtlPairDict[pair]
            for p in pList:
                noReciprocalList.append(p)
            hasFatalErrors = 1
    return 0

# end runQcChecks() -------------------------------

def writeLoadReadyFile():
    for a in allelesToLoadList:
        fpLoadReady.write(a.toLoad())

    return 0

# end writeLoadReadyFile() -------------------------------

#
# Main
#
print('checkArgs(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
checkArgs()

print('init(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
init()

#print('runQcChecks(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
#sys.stdout.flush()
#runQcChecks()

#print('writeReport(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
#sys.stdout.flush()
#writeReport()

# everything is fatal right now - keep to see if we will need
#print('writeLoadReadyFile(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
#sys.stdout.flush()
#writeLoadReadyFile()

print('closeFiles(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
closeFiles()

db.useOneConnection(0)
print('done: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))

if hasFatalErrors == 1 :
    sys.exit(2)
else:
    sys.exit(0)

