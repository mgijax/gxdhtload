
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

# actions expected:
actionList = ['add', 'update']

#
# Lists for reporting
#

# AE experiments with configured prefixes in the database
exptsInDbList = []

# AE experiments with configured  prefixes in the database with samples
exptsInDbWithSampleList = []

# IDs seen in the input file
distinctIdList = []

# duplicated expt IDs in the input
dupeIDList = []

# lines with < 2 columns
missingColumnList = []

# experiment ID is not valid
badIdList = []

# action in file not expected
badActionList = []

# add action and expt ID in the db
addInDbList = []

# update action and expt ID not in the db
updateNotInDbList = []

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
    global exptsInDbList, exptsInDbWithSampleList

    # open input/output files
    openFiles()

    # create a list and query string from the configured prefix(es)
    exptPrefixList = list(map(str.strip, str.split(exptPrefixes, ',')))

    exptQueryString = "', '".join([str(elem) for elem in exptPrefixList])

    # add sgl quotes to the beginning and end
    exptQueryString = "'%s'" % exptQueryString

    db.sql('''select accid as exptID, e._experiment_key
        into temporary table aeExpts
        from acc_Accession a, gxd_htexperiment e
        where a._mgitype_key = 42
        and a._logicaldb_key = 189
        and a.prefixpart in (%s)
        and a._object_key = e._experiment_key''' % exptQueryString, None)
    db.sql('''create index idx1 on aeExpts(_experiment_key)''', None)

    results1 = db.sql('''select * from aeExpts''', 'auto')
    results2 =  db.sql('''select a.*
        from aeExpts a, gxd_htrawsample s
        where a._experiment_key = s._experiment_key''', 'auto')

    #print(len(results1))
    for r in results1:
        exptsInDbList.append(r['exptID'])
    #print('len(exptsInDbList): %s' % len(exptsInDbList))

    #print(len(results2))
    for r in results2:
        exptsInDbWithSampleList.append(r['exptID'])

    #print('len(exptsInDbWithSampleList): %s' % len(exptsInDbWithSampleList))
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

    if len(dupeIDList):
        print('dupeIDList: %s' % dupeIDList)
        fpQcRpt.write(CRT + CRT + str.center('Experiment IDs Duplicated In Input',60) + CRT)
        fpQcRpt.write('%-5s  %-20s%s' % ('Line#','ID', CRT))
        fpQcRpt.write(5*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(dupeIDList))
        fpQcRpt.write(CRT + 'Total: %s' % len(dupeIDList))

    if len(missingColumnList):
        fpQcRpt.write(CRT + CRT + str.center('Lines with < 2 Columns',60) + CRT)
        fpQcRpt.write('%-5s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(5*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(missingColumnList))
        fpQcRpt.write(CRT + 'Total: %s' % len(missingColumnList))

    if len(badIdList):
        fpQcRpt.write(CRT + CRT + str.center('Invalid Experiment ID Prefix',60) + CRT)
        fpQcRpt.write('%-5s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(5*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(badIdList))
        fpQcRpt.write(CRT + 'Total: %s' % len(badIdList))

    if len(badActionList):
        fpQcRpt.write(CRT + CRT + str.center('Invalid Action',60) + CRT)
        fpQcRpt.write('%-5s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(5*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(badActionList))
        fpQcRpt.write(CRT + 'Total: %s' % len(badActionList))

    if len(addInDbList):
        fpQcRpt.write(CRT + CRT + str.center('Add Action, Experiment in Database',60) + CRT)
        fpQcRpt.write('%-5s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(5*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(addInDbList))
        fpQcRpt.write(CRT + 'Total: %s' % len(addInDbList))

    if len(updateNotInDbList):
        fpQcRpt.write(CRT + CRT + str.center('Update Action, Experiment not in Database',60) + CRT)
        fpQcRpt.write('%-5s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(5*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(updateNotInDbList))
        fpQcRpt.write(CRT + 'Total: %s' % len(updateNotInDbList))

    if len(exptsInDbWithSampleList):
        fpQcRpt.write(CRT + CRT + str.center('Update Action, Experiment in Database with Samples',60) + CRT)
        fpQcRpt.write('%-5s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(5*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(exptsInDbWithSampleList))
        fpQcRpt.write(CRT + 'Total: %s' % len(exptsInDbWithSampleList))

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
    global hasFatalErrors, distinctIdList, dupeIDList, addInDbList
    global missingColumnList, badActionList, badIdList, updateNotInDbList

    line = fpInput.readline()
    lineNum = 0 
    while line:
        lineNum += 1
        #print('lineNum: %s line: %s' % (lineNum, line))

        # check that the file has at least 2 columns
        if len(str.split(line, TAB)) < 2:
            print('missing column in: %s' % line)
            missingColumnList.append('%s  %s' % (lineNum, line))
            hasFatalErrors = 1
            line = fpInput.readline()
            continue
        # get columns 1-2 
        (exptID, action) = list(map(str.strip, str.split(line, TAB)))[:2]
        print('exptID: %s action: %s' % (exptID, action))

        if exptID not in distinctIdList:
            distinctIdList.append(exptID)
        else:
            dupeIDList.append('%s  %s' % (lineNum, line))
            hasFatalErrors = 1 
        # for case insensitive QC
        action = str.lower(action)

        # Now verify each column
        # Col 1 - expt ID
        found = 0
        for p in exptPrefixList:
            #print('prefix: %s' % p)
            if str.find(exptID, p) != -1:
                found = 1 
                break

        if found == 0:
            badIdList.append('%s  %s' % (lineNum, line))
            hasFatalErrors = 1    
            line = fpInput.readline()
            hasFatalErrors = 1
            continue

        # Col 2 - action
        if action not in actionList:
            badActionList.append('%s  %s' % (lineNum, line))
            hasFatalErrors = 1

        # if no fatal errors thus far check the action agains the db

        # adds should not be in the database
        if action == 'add' and exptID in exptsInDbList:
            addInDbList.append('%s  %s' % (lineNum, line))
            hasFatalErrors = 1

        # updates should be in the database
        if action == 'update' and exptID not in exptsInDbList:
            updateNotInDbList.append('%s  %s' % (lineNum, line))
            hasFatalErrors = 1

        # updates should not have raw samples in the database
        if action == 'update' and exptID in exptsInDbWithSampleList:
            updateWithSamplesInDbList.append('%s  %s' % (lineNum, line))
            hasFatalErrors = 1

        line = fpInput.readline()
    return 0

# end runQcChecks() -------------------------------

#
# Main
#
print('checkArgs(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
checkArgs()

print('init(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
init()

print('runQcChecks(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
runQcChecks()

print('writeReport(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
writeReport()

print('closeFiles(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
closeFiles()

db.useOneConnection(0)
print('done: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))

if hasFatalErrors == 1 :
    sys.exit(2)
else:
    sys.exit(0)

