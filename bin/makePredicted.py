#!/usr/bin/env python3
'''
  Purpose:
           run sql to get unevaluated GEO experiments that need to be
           run through gxdhtclassifier
           (minor) Data transformations include:
            replacing non-ascii chars with ' '
            replacing FIELDSEP and RECORDSEP chars in the doc text w/ ' '

  Outputs:      Delimited file to file system
                See htMLsample.HtSample for output format
                Writes progress updates to stderr unless you use -q
                Writes raw sample text transformation report to the specified
                    file if you use the --report filename option
                Return code is 0 if no errors.
'''
import sys
import os
import time
import argparse
import db
import htMLsample as mlSampleLib
import htRawSampleTextManager
from utilsLib import removeNonAscii
#-----------------------------------

sampleObjType = mlSampleLib.HtSample

# for the Sample output file
RECORDEND    = sampleObjType.getRecordEnd()
FIELDSEP     = sampleObjType.getFieldSep()
#-----------------------------------

def getArgs():

    parser = argparse.ArgumentParser( \
        description='Get unevaluated GEO experiments to run through gxdhtclassifier, write to stdout')

    parser.add_argument('-l', '--limit', dest='nResults',
        required=False, type=int, default=0, 		# 0 means ALL
        help="limit results to n GEO experiments. Default is no limit")

    parser.add_argument('--report', dest='reportFile', action='store',
        required=False, default=None,
        help='Write raw sample text manager transformation report to this file')

    parser.add_argument('-q', '--quiet', dest='verbose', action='store_false',
        required=False, help="skip helpful messages to stderr")

    defaultHost = os.environ.get('PG_DBSERVER', 'bhmgidevdb01')
    defaultDatabase = os.environ.get('PG_DBNAME', 'prod')

    parser.add_argument('-s', '--server', dest='server', action='store',
        required=False, default=defaultHost,
        help='db server. Shortcuts:  adhoc, prod, dev, test. (Default %s)' %
                defaultHost)

    parser.add_argument('-d', '--database', dest='database', action='store',
        required=False, default=defaultDatabase,
        help='which database. Example: mgd (Default %s)' % defaultDatabase)

    args =  parser.parse_args()

    if args.server == 'adhoc':
        args.host = 'mgi-adhoc.jax.org'
        args.db = 'mgd'
    elif args.server == 'prod':
        args.host = 'bhmgidb01.jax.org'
        args.db = 'prod'
    elif args.server == 'dev':
        args.host = 'mgi-testdb4.jax.org'
        args.db = 'jak'
    elif args.server == 'test':
        args.host = 'bhmgidevdb01.jax.org'
        args.db = 'prod'
    else:
        args.host = args.server
        args.db = args.database

    return args
#-----------------------------------

args = getArgs()

#-----------------------------------
GEO_TMPTBL = 'tmp_geoexp' # name of tmp table w/ the uneval'ed GEO experiments

def loadTmpTable():
    '''
    Select the appropriate HT experiments to be used and put them in the
    tmp tables. Columns:
        _experiment_key
        ID (GEO ID if available)
        title
        description
    '''
    # Populate GEO_TMPTBL: unevaluated GEO experiments
    q = ["""
        create temporary table %s as
        select e._experiment_key, a.accid as ID, e.name as title, e.description
        from gxd_htexperiment e
            join acc_accession a on
                (a._object_key = e._experiment_key and a._mgitype_key = 42
                and a._logicaldb_key = 190) -- GEO series
        where
        e._evaluationstate_key = 100079348 -- 'Not Evaluated'
        """ % (GEO_TMPTBL),
        """
        create index tmp_idx1 on %s(_experiment_key)
        """ % (GEO_TMPTBL),
        ]
    results = db.sql(q, 'auto')
#-----------------------------------

def main():
    #db.set_sqlServer  (args.host)
    #db.set_sqlDatabase(args.db)
    #db.set_sqlUser    ("mgd_public")
    #db.set_sqlPassword("mgdpub")

    fpOut = open(os.getenv('NOT_EVALUATED_EXPERIMENT'), 'w')
    startTime = time.time()
    verbose("%s\nHitting database %s %s as mgd_public\n" % \
                                        (time.ctime(), args.host, args.db,))
    loadTmpTable()
    rstm = htRawSampleTextManager.RawSampleTextManager(db,expTbl=GEO_TMPTBL)

    q = """select * from %s\n""" % (GEO_TMPTBL)
    if args.nResults != 0:
        limitClause = 'limit %d\n' % args.nResults
        q += limitClause

    results = db.sql(q, 'auto')
    verbose("got %d experiments from db\n" % len(results))

    outputSampleSet = mlSampleLib.SampleSet(sampleObjType=sampleObjType)
    for i,r in enumerate(results):
        try:
            expKey = r['_experiment_key']
            rawSampleText = rstm.getRawSampleText(expKey)

            sample = sqlRecord2HtSample(r, rawSampleText)
            outputSampleSet.addSample(sample)
        except:         # if some error, try to report which record
            sys.stderr.write("Error on record %d:\n%s\n" % (i, str(r)))
            raise

    outputSampleSet.setMetaItem('host', args.host)
    outputSampleSet.setMetaItem('db', args.db)
    outputSampleSet.setMetaItem('time', time.strftime("%Y/%m/%d-%H:%M:%S"))

    outputSampleSet.write(fpOut)
    fpOut.close()
    
    verbose("wrote %d experiments\n" % outputSampleSet.getNumSamples())
    if args.reportFile:
        fp = open(args.reportFile, 'w')
        fp.write(rstm.getReport())
        verbose("wrote raw sample text transformation report to '%s'\n" \
                                                            % args.reportFile)
    verbose("%8.3f seconds\n\n" %  (time.time()-startTime))

    return
#-----------------------------------

def sqlRecord2HtSample(r,               # sql Result record
                       rawSampleText,   # text from raw sample metadata 
    ):
    """
    Encapsulates knowledge of HtSample.setFields() field names
    """
    newR = {}
    newSample = sampleObjType()

    if len(rawSampleText) > 0:          # add separator to mark beginning
        rawSampleText = cleanUpTextField(" .. " + rawSampleText)

    ## populate the Sample fields
    newR['ID']          = str(r['id'])
    newR['title']       = cleanUpTextField(r['title'])
    newR['description'] = cleanUpTextField(r['description']) +rawSampleText

    return newSample.setFields(newR)
#-----------------------------------

def cleanUpTextField(text):
    if text == None:
        text = ''
    text = removeNonAscii(cleanDelimiters(text))
    return text
#-----------------------------------

def cleanDelimiters(text):
    """ remove RECORDEND and FIELDSEPs from text (replace w/ ' ')
    """
    return text.replace(RECORDEND,' ').replace(FIELDSEP,' ')
#-----------------------------------

def verbose(text):
    if args.verbose:
        sys.stderr.write(text)
        sys.stderr.flush()
#-----------------------------------

if __name__ == "__main__":
    main()
