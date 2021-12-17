#
# Report:
#       find all xml tags
#
# History:
#
# sc	12/03/20
#	- created
#
 
import sys 
import os
import db
import reportlib
import subprocess

db.setTrace()

CRT = reportlib.CRT
SPACE = reportlib.SPACE
TAB = reportlib.TAB
PAGE = reportlib.PAGE

GEO_DOWNLOADS = os.getenv('GEO_DOWNLOADS')

# the list of geo sample Ids from all the metadata files
geoSampleIdList = []

# plug GEO ids into this template to get the sample data files
# example: ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE62nnn/GSE62608/miniml/GSE62608_family.xml.tgz

# GEO data are available for download from the FTP site. Directory structure is organized by type, GEO accession range, GEO accession number, and format. Range subdirectory name is created by replacing the three last digits of the accession with letters "nnn". For example,
#  GSM575: /samples/GSMnnn/GSM575/
#  GSM1234: /samples/GSM1nnn/GSM1234/
#  GSM12345: /samples/GSM12nnn/GSM12345/

ftpUrlTemplate = "ftp://ftp.ncbi.nlm.nih.gov/geo/series/~x~/~id~/miniml/~id~_family.xml.tgz"
ftpFileTemplate = "~id~_family.xml.tgz"

#
# Main
#

#
# Purpose: Loops through all metadata files sending them to parser
# Returns:
# Assumes: Nothing
# Effects:
# Throws: Nothing
#
def parseAll():
    fpInFile = None
    print('ALL_FILES: %s' % os.environ['ALL_FILES'])
    for file in str.split(os.environ['ALL_FILES']):
        try:
            fpInFile = open(file, 'r')
        except:
            print('%s does not exist' % inFileName)
        parseFile(fpInFile)
        fpInFile.close()
    return

# parse a geo metadata file
def parseFile(fpInFile):
    global geoSampleIdList
    expFound = 0
    gseId = ''
    line = str.lstrip(fpInFile.readline())
    # don't strip if just newline so blank lines won't stop the loop
    if line != '\n':
         line = str.strip(line)

    while line:
        if line.find('<DocumentSummary ') == 0:
            expFound = 1
        elif line.find('</DocumentSummary>') == 0:
            expFound = 0
        #print 'expFound: %s' % expFound
        if expFound:
            if line.find('<Accession') == 0:
                id =  line.split('>')[1].split('<')[0]
                if id.find('GSE') == 0:
                    geoSampleIdList.append(id)
        line = fpInFile.readline()
        # don't strip if just newline so blank lines won't stop the loop
        if line != '\n':
             line = str.strip(line)
    return

# iterate thru the geo IDs fetching the family files from the ftp site
def process():
    for id in geoSampleIdList:

        # create url
        #print(id)
        if len(id) == 4:
            xPart = id[:-1] + 'nnn'
        elif len(id) == 5:
            xPart = id[:-2] + 'nnn'
        else:
            xPart = id[:-3] + 'nnn'
        #print('xPart: %s' % xPart)
        url = ftpUrlTemplate.replace('~x~', xPart)
        url = url.replace('~id~', id)
        file = ftpFileTemplate.replace('~id~', id)
        unzipped = '%s/%s' % (GEO_DOWNLOADS, file[:-4])
        #print('file: %s' % file)
        print(url)

        # checked to see if the unzipped file exists, if it does don't unzip
        isFile = os.path.isfile(unzipped)
        if isFile:
            print('Already exists: %s' % unzipped)
            continue

        # create command and run it
        # -nc no clobber - if the file exists, don't overwrite it
        cmd = 'wget -nc -O %s/%s %s' % (GEO_DOWNLOADS, file, url)
        print('cmd: %s' % cmd)
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        stdout = result.stdout
        stderr = result.stderr
        statusCode = result.returncode
        print('wget statusCode: %s stderr: %s' % (statusCode, stderr))

        # untar/unzip the file
        cmd = '/usr/bin/tar -xzvf %s/%s ' % (GEO_DOWNLOADS, file) 
        print(cmd)
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        stdout = result.stdout
        stderr = result.stderr
        statusCode = result.returncode
        print('tar statusCode: %s stderr: %s' % (statusCode, stderr))

	# remove extraneous files from the tarball and the 
	# *.tgz, GPL*, GSM*
        cmd = '/usr/bin/rm -f %s/%s ' % (GEO_DOWNLOADS, file)
        print(cmd)
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        stdout = result.stdout
        stderr = result.stderr
        statusCode = result.returncode
        print('rm statusCode: %s stderr: %s' % (statusCode, stderr))

        cmd = '/usr/bin/rm -f %s/GPL* ' % (GEO_DOWNLOADS)
        print(cmd)
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        stdout = result.stdout
        stderr = result.stderr
        statusCode = result.returncode
        print('rm statusCode: %s stderr: %s' % (statusCode, stderr))

        cmd = '/usr/bin/rm -f %s/GSM* ' % (GEO_DOWNLOADS)
        print(cmd)
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        stdout = result.stdout
        stderr = result.stderr
        statusCode = result.returncode
        print('rm statusCode: %s stderr: %s' % (statusCode, stderr))

    return

### main ###
parseAll()
print('Number GEO Ids to process: %s' % len(geoSampleIdList))
#print(geoIdList)
process()
