#
# Download experiments 
#
# History:
#
# sc	12/22/23
#	- created
#
 
import sys 
import os
import reportlib
import subprocess
import loadlib
import Set

CRT = reportlib.CRT
SPACE = reportlib.SPACE
TAB = reportlib.TAB
S = '%s'

inputDir = os.getenv('INPUTDIR')
inputFile =  os.getenv('INPUT_FILE_DEFAULT')

curLogName = os.getenv('MIRROR_LOG_CUR')

#
# Create the path and file templates
#
baseExpUrl = os.getenv('BASE_EXP_URL')
baseSampUrl = os.getenv('BASE_SAMP_URL')

# Example: https://ftp.ebi.ac.uk/biostudies/fire/E-MTAB-/993/E-MTAB-14993/E-MTAB-14993.json
# Samp Example: https://www.ebi.ac.uk/biostudies/files/E-MTAB-14993/E-MTAB-14993.sdrf.txt
ftpExpUrlTemplate = baseExpUrl + '/%s/%s/%s/%s.json'
expFileTemplate = "%s.json"

ftpSampUrlTemplate = '%s%s/%s.sdrf.txt' % (baseSampUrl, S, S)
ftpSampFileTemplate = '%s.sdrf.txt'
print('ftpExpUrlTemplate: %s ftpSampUrlTemplate: %s' % (baseExpUrl, ftpSampUrlTemplate))

fpIn = None

# the list of geo experiment Ids for which we want to fetch files
expIdList = []

# set of distinct file header tokens
headerSet = set([])

# set of distinct column headers

# plug GEO ids into this template to get the sample data files
# example: ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE62nnn/GSE62608/miniml/GSE62608_family.xml.tgz

def init():
    global fpIn, expIdList, fpCurLogFile

    try:
        fpIn = open(inputFile, 'r')
    except:
        print('%s does not exist' % inputFile)
        sys.exit(1)

    try:
        fpCurLogFile = open(curLogName, 'a')  # append as this is started in mirror_ae.sh  
        fpCurLogFile.write('%s%s%s' % (loadlib.loaddate, CRT, CRT))
    except:
        print('Cannot create %s' % curLogName)
        sys.exit(1)

    for line in fpIn.readlines():
        (exptID, action) = list(map(str.strip, str.split(line, TAB)))[:2]
        expIdList.append(exptID)

    return

# iterate thru the ArrayExpress IDs fetching the experiment and sample files
def process():
    for id in expIdList:

        # create url
        #print(id)

        # Example: https://ftp.ebi.ac.uk/biostudies/fire/E-MTAB-/993/E-MTAB-14993/E-MTAB-14993.json
        # wts2-1664/e4g-256  - BASE_EXPT_URL changed
        tokens = id.split('-')
        t1 = tokens[0] + '-' + tokens[1] + '-'
        t2 = tokens[2]
        n = 3
        t3 = ""
        for i in range(len(t2) - n, len(t2)):
            t3 += t2[i]
        expURL = ftpExpUrlTemplate % (t1, t3, id, id)
        #print(expURL)

        expFile = expFileTemplate % id
        smpURL = ftpSampUrlTemplate % (id, id)
        smpFile = ftpSampFileTemplate % id

        print('%sProcessing: %s%s' % (CRT, id, CRT)) 
        print('expURL: %s%s expFile: %s/%s%s smpURL: %s%s smpFile: %s/%s%s' % (expURL, CRT, inputDir, expFile, CRT, smpURL, CRT, inputDir, smpFile, CRT))

        # create experiment command and run it
        cmd = 'wget -O %s/%s %s' % (inputDir, expFile, expURL)
        print('cmd: %s' % cmd)
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        stdout = result.stdout
        stderr = result.stderr
        statusCode = result.returncode

        if statusCode != 0:
            fpCurLogFile.write('%sExperiment file: %s%s' % (CRT, expFile, CRT))
            fpCurLogFile.write('%s failed with exit code %s \nstderr %s' % (cmd, statusCode, stderr))
            print('Skipping %s' % (expFile))

            cmd = '/usr/bin/rm -f %s/%s ' % (inputDir, expFile)
            print(cmd)
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            continue
        else:
            print('Experiment file: %s successfully downloaded' % expFile)

        # -nc no clobber - if the file exists, don't overwrite it

        # create experiment command and run it
        cmd = 'wget -nc -O %s/%s %s' % (inputDir, smpFile, smpURL, )

        print('cmd: %s' % cmd)
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        stdout = result.stdout
        stderr = result.stderr
        statusCode = result.returncode

        if statusCode != 0 and str.find(stderr, 'already there') == -1:
            fpCurLogFile.write('%sSample file: %s%s' % (CRT, smpFile, CRT))
            fpCurLogFile.write('%s failed with exit code %s \nstderr %s' % (cmd, statusCode, stderr))
            print('Skipping %s' % (smpFile))

            cmd = '/usr/bin/rm -f %s/%s ' % (inputDir, smpFile)
            print(cmd)
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            continue
        else:
            print('Sample file: %s successfully downloaded' % smpFile)
            print('    %s' % stderr)
            continue

### main ###
print('init')
init()

print('Number Ids to fetch: %s' % len(expIdList))
print(expIdList)

print('process')
process()
fpCurLogFile.write('%s%s%s' % (CRT, CRT, loadlib.loaddate))
fpCurLogFile.close()

