#!/usr/bin/env python
b'This script requires python 3.4'

"""
Crawler which runs over all HPSS picoDST folder for now and populates
mongoDB collections.

HPSSFiles: Is a collection of all files within those folders
-> This is the true represetation on what is on tape.
Every time the crawler runs, it updates the lastSeen field

unique index is: fileFullPath

fileType can be: tar, idx, picoDst, other

This is a typical document:
{'_id': ObjectId('5723e67af157a6a310232458'),
 'fileSize': '13538711552',
 'fileType': 'tar',
 'filesInTar': 23,
 'fileFullPath': '/nersc/projects/starofl/picodsts/Run10/AuAu/11GeV/all/P10ih/148.tar',
  'lastSeen': '2016-04-29'}

HPSSPicoDsts: Is a collection of all picoDsts stored on HPSS,
-> Every picoDst should show up only once. Duplicate entries are caught seperatly (see below)

unique index is: filePath

This is a typical document:
{'_id': 'Run10/AuAu/11GeV/all/P10ih/149/11149081/st_physics_adc_11149081_raw_2520001.picoDst.root',
 'filePath': 'Run10/AuAu/11GeV/all/P10ih/149/11149081/st_physics_adc_11149081_raw_2520001.picoDst.root',
 'fileSize': '5103599',
 'fileFullPath': '/project/projectdirs/starprod/picodsts/Run10/AuAu/11GeV/all/P10ih/149/11149081/st_physics_adc_11149081_raw_2520001.picoDst.root',
 'dataClass': 'picoDst',
 'isInTarFile': True,
 'fileFullPathTar': '/nersc/projects/starofl/picodsts/Run10/AuAu/11GeV/all/P10ih/149.tar',
 'starDetails': {'runyear': 'Run10',
                 'system': 'AuAu',
                 'energy': '11GeV',
                 'trigger': 'all',
                 'production': 'P10ih',
                 'day': 149,
                 'runnumber': 11149081,
                 'stream': 'st_physics_adc',
                 'picoType': 'raw'},
 'staging': {'stageMarkerXRD': False}}

HPSSDuplicates: Collection of duplicted picoDsts on HPSS

"""

import sys
import os
import re

import logging as log
import time
import socket
import datetime
import shlex, subprocess

from mongoUtil import mongoDbUtil
import pymongo

from pymongo import results
from pymongo import errors
from pymongo import bulk

from pprint import pprint

##############################################
# -- GLOBAL CONSTANTS

HPSS_BASE_FOLDER = "/nersc/projects/starofl"
PICO_FOLDERS     = [ 'picodsts', 'picoDST' ]

##############################################

# -- Check for a proper Python Version
if sys.version[0:3] < '3.0':
    print ('Python version 3.0 or greater required (found: {0}).'.format(sys.version[0:5]))
    sys.exit(-1)

# ----------------------------------------------------------------------------------
class hpssUtil:
    """Helper Class for HPSS connections and retrieving stuff"""

    # _________________________________________________________
    def __init__(self, dataClass = 'picoDst', pathKeysSchema = 'runyear/system/energy/trigger/production/day%d/runnumber'):
        #    def __init__(self, dataClass = 'picoDst', pathKeysSchema = 'runyear/system/energy/trigger/production/day%d/runnumber%d'):
        self._today = datetime.datetime.today().strftime('%Y-%m-%d')

        self._dataClass        = dataClass
        self._fileSuffix       = '.{0}.root'.format(dataClass)
        self._lengthFileSuffix = len(self._fileSuffix)

        if dataClass == 'picoDst':
            pathKeys = pathKeysSchema.split(os.path.sep)

            # -- Get the type from each path key (tailing % char), or 's' for
            #    string if absent.  i.e.
            #    [['runyear', 's'], ['system', 's'], ['day', 'd'], ['runnumber', 'd']]
            self._typedPathKeys = [k.split('%') if '%' in k else [k, 's'] for k in pathKeys]
            self._typeMap = {'s': str, 'd': int, 'f': float}

    # _________________________________________________________
    def _getTypedPathKeys(self, tokenizedPath):
        """Get typed path keys for different scenarios."""

        # -- Default case
        if len(tokenizedPath) == 8:
            return self._typedPathKeys

        elif len(tokenizedPath) == 7:
            pathKeysSchema = 'runyear/system/energy/trigger/production/day%d/runnumber'

            # _________________________________________________________
            def _getDateIndex(tokenizedPath):
                """Get index of date field."""

                for idx in range(len(tokenizedPath)):
                    isDate = True
                    try:
                        date = int(tokenizedPath[idx])
                        if date > 370:
                            isDate = False
                    except ValueError:
                        isDate = False

                    if isDate:
                        return idx
                return -1

            dateIdx = _getDateIndex(tokenizedPath)

            if dateIdx == 4 and "GeV" in tokenizedPath[2]:
                # ORIG: pathKeysSchema = 'runyear/system/energy/trigger/production/day%d/runnumber'
                pathKeysSchema = 'runyear/system/energy/trigger/day%d/runnumber'

                pathKeys = pathKeysSchema.split(os.path.sep)
                typedPathKeys = [k.split('%') if '%' in k else [k, 's'] for k in pathKeys]
                return typedPathKeys

            else:
                print("SCHEMA NOT KNOWN !!! - use Default", tokenizedPath)
                return self._typedPathKeys

        else:
            print("SCHEMA NOT KNOWN !!! - use Default", tokenizedPath)
            return self._typedPathKeys

    # _________________________________________________________
    def setCollections(self, collHpssFiles, collHpssPicoDsts, collHpssDuplicates):
        """Get collection from mongoDB."""

        self._collHpssFiles      = collHpssFiles
        self._collHpssPicoDsts   = collHpssPicoDsts
        self._collHpssDuplicates = collHpssDuplicates

    # _________________________________________________________
    def getFileList(self):
        """Loop over both folders containing picoDSTs on HPSS."""

        for picoFolder in PICO_FOLDERS:
            self._getFolderContent(picoFolder)
            break

    # _________________________________________________________
    def _getFolderContent(self, picoFolder):
        """Get listing of content of picoFolder."""

        # -- Get subfolders from HPSS
        cmdLine = 'hsi -q ls -1 {0}/{1}'.format(HPSS_BASE_FOLDER, picoFolder)
        cmd = shlex.split(cmdLine)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        # -- Loop of the list of subfolders
        for subFolder in iter(p.stdout.readline, b''):
            if "Run" in subFolder.decode("utf-8").rstrip():
                print("SubFolder: ", subFolder.decode("utf-8").rstrip())
                self._parseSubFolder(subFolder.decode("utf-8").rstrip())

    # _________________________________________________________
    def _parseSubFolder(self, subFolder):
        """Get recursive list of folders and files in subFolder ... as "ls" output."""

        cmdLine = 'hsi -q ls -lR {0}'.format(subFolder)
        cmd = shlex.split(cmdLine)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        # -- Parse ls output line-by-line -> utilizing output blocks in ls
        inBlock = 0
        listPicoDsts = []
        for lineTerminated in iter(p.stdout.readline, b''):
            line = lineTerminated.decode("utf-8").rstrip('\t\n')
            lineCleaned = ' '.join(line.split())

            if lineCleaned.startswith(subFolder):
                inBlock = 1
                self._currentBlockPath = line.rstrip(':')
            else:
                if not lineCleaned:
                    inBlock = 0
                    self._currentBlockPath = ""
                else:
                    if inBlock and not lineCleaned.startswith('d'):
                        doc = self._parseLine(lineCleaned)

                        # -- update lastSeen and insert if not in yet
                        ret = self._collHpssFiles.find_one_and_update({'fileFullPath': doc['fileFullPath']},
                                                                      {'$set': {'lastSeen': self._today},
                                                                       '$setOnInsert' : doc},
                                                                       upsert = True)

                        # -- document already there do nothing
                        if ret:
                            continue

                        # -- new document inserted - add the picoDst(s)
                        if doc['fileType'] == "tar":
                            nDocsInTar = self._parseTarFile(doc)
                            self._collHpssFiles.find_one_and_update({'fileFullPath': doc['fileFullPath']},
                                                                    {'$set': {'filesInTar': nDocsInTar}})
                            continue

                        if doc['fileType'] == "picoDst":
                            listPicoDsts.append(self._makePicoDstDoc(doc['fileFullPath'], doc['fileSize']))

                            if len(listPicoDsts) >= 10000:
                                self._insertPicoDsts(listPicoDsts)
                                listPicoDsts[:] = []

        # -- Insert picoDsts in collection
        self._insertPicoDsts(listPicoDsts)

    # _________________________________________________________
    def _parseLine(self, line):
        """Parse one entry in HPSS subfolder.

           Get every file with full path, size, and details
           """

        lineTokenized = line.split(' ', 9)

        fileName     = lineTokenized[8]
        fileFullPath = "{0}/{1}".format(self._currentBlockPath, fileName)
        fileSize     = int(lineTokenized[4])
        fileType     = "other"

        if fileName.endswith(".tar"):
            fileType = "tar"
        elif fileName.endswith(".idx"):
            fileType = "idx"
        elif fileName.endswith(".picoDst.root"):
            fileType = "picoDst"

        # -- return record
        return { 'fileFullPath': fileFullPath, 'fileSize': fileSize, 'fileType': fileType}

    # _________________________________________________________
    def _parseTarFile(self, hpssDoc):
        """Get Content of tar file and parse it.

           return a number of documents in Tar file
           """

        cmdLine = 'htar -tf {0}'.format(hpssDoc['fileFullPath'])
        cmd = shlex.split(cmdLine)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        listDocs = []

        for lineTerminated in iter(p.stdout.readline, b''):
            line = lineTerminated.decode("utf-8").rstrip('\t\n')
            lineCleaned = ' '.join(line.split())

            if lineCleaned == "HTAR: HTAR SUCCESSFUL" or \
                    lineCleaned.startswith('HTAR: d'):
                continue

            if 'ERROR: No such file: {0}.idx'.format(hpssDoc['fileFullPath']) == lineCleaned :
                print('ERROR no IDX file ...', lineCleaned, '... recovering')
                return

            lineTokenized = lineCleaned.split(' ', 7)

            if len(lineTokenized) < 7:
                print("Error tokenizing hTar line:", lineTokenized)
                continue

            fileFullPath  = lineTokenized[6]
            fileSize      = int(lineTokenized[3])

            # -- select only dataClass
            if not fileFullPath.endswith(self._fileSuffix):
                continue

            # -- make PicoDst document and add it to list
            listDocs.append(self._makePicoDstDoc(fileFullPath, fileSize, hpssDoc=hpssDoc, isInTarFile=True))

        nDocsInTar = len(listDocs)

        # -- Insert picoDsts in collection
        self._insertPicoDsts(listDocs)

        return nDocsInTar

    # _________________________________________________________
    def _makePicoDstDoc(self, fileFullPath, fileSize, hpssDoc=None, isInTarFile=False):
        """Create entry for picoDsts."""

        # -- identify start of "STAR naming conventions"
        idxBasePath = fileFullPath.find("/Run")+1

        # -- Create document
        doc = {
               'filePath':     fileFullPath[idxBasePath:],
               'fileFullPath': fileFullPath,
               'fileSize':     fileSize,
               'dataClass':    self._dataClass,
               'isInTarFile':  isInTarFile,
               'staging':      { 'stageMarkerXRD': False},
               'isInRunBak':   False
            }

        if isInTarFile:
            doc['fileFullPathTar'] = hpssDoc['fileFullPath']

        # -- Strip basePath of fileName and tokenize it
        cleanPathTokenized = doc['filePath'].split(os.path.sep)

        # -- Get TypedKeys for tokenized path
        typedPathKeys = self._getTypedPathKeys(cleanPathTokenized)

        # -- Create STAR details sub document
        docStarDetails = dict([(keys[0], self._typeMap[keys[1]](value))
                               for keys, value in zip(typedPathKeys, cleanPathTokenized)])

        # -- remove ".bak" from runyear and _id / fileType (for the uniqueness of the picoDst)
        if '.' in docStarDetails['runyear']:
            splitRunYear = docStarDetails['runyear'].split('.')
            docStarDetails['runyear'] = splitRunYear[0]
            bakString = ".{0}".format(splitRunYear[1])
            doc['filePath'] = doc['filePath'].replace(bakString, '')
            doc['isInRunBak'] = True

        # -- Create a regex pattern to get the stream from the fileName
        regexStream = re.compile('(st_.*)_{}'.format(docStarDetails.get('runnumber', '')))

        fileNameParts = re.split(regexStream, cleanPathTokenized[-1])
        if len(fileNameParts) == 3 and len(fileNameParts[0]) == 0:
            docStarDetails['stream'] = fileNameParts[1]

            strippedSuffix = fileNameParts[-1][1:-self._lengthFileSuffix]
            strippedSuffixParts = strippedSuffix.split('_')

            docStarDetails['picoType'] = strippedSuffixParts[0] \
                if len(strippedSuffixParts) == 2 \
                else strippedSuffix
        else:
            print('xxx: ', fileNameParts, docStarDetails)
            docStarDetails['stream'] = 'xx'
            docStarDetails['picoType'] = 'xx'

        # -- Add STAR details to document
        doc['starDetails'] = docStarDetails

        # -- return picoDst document
        return doc


    # _________________________________________________________
    def _insertPicoDsts(self, listDocs):
        """Insert list of picoDsts in to collections.

        In HPSSPicoDst collection and
        in to HPSSDuplicates collection if a duplicate
        """

        # -- Empty list
        if not listDocs:
            return

#        print("Insert List: Try to add {0} picoDsts".format(len(listDocs)))

        # -- Clean listDocs with duplicate entries and move them on extra list: listDuplicates
        listDuplicates = []

        for entry in self._collHpssPicoDsts.find({'starDetails.runyear': listDocs[0]['starDetails']['runyear']}, {'filePath': True, '_id': False}):
            element = next((item for item in listDocs if item['filePath'] == entry['filePath']), None)
            if element:
                listDuplicates.append(element)
                listDocs.remove(element)

        # -- Insert list of picoDsts in to HpssPicoDsts collection
        if listDocs:
            print("Insert List: Add {0} picoDsts".format(len(listDocs)))
            self._collHpssPicoDsts.insert_many(listDocs, ordered=False)

        # -- Insert list of duplicate picoDsts in to HpssDuplicates collection
        if listDuplicates:
            print("Insert List: Add {0} duplicate picoDsts".format(len(listDuplicates)))
            self._collHpssDuplicates.insert_many(listDuplicates, ordered=False)


# ____________________________________________________________________________
def checkForHPSSTransfer():
    """Check for ongoing transfer of files into HPSS"""

    cmdLine = 'qstat -u starofl'
    cmd = shlex.split(cmdLine)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    for lineTerminated in iter(p.stdout.readline, b''):
        line = lineTerminated.decode("utf-8").rstrip('\t\n')
        lineCleaned = ' '.join(line.split())

        if "tarToHPSS" in lineCleaned:
            return True

    return False

# ____________________________________________________________________________
def main():
    """initialize and run"""

    # -- Check for ongoing transfer into HPSS
    if checkForHPSSTransfer():
        print ("Abort - Data is currently moved to HPSS")
        return

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

    collHpssFiles      = dbUtil.getCollection("HPSS_Files")
    collHpssPicoDsts   = dbUtil.getCollection("HPSS_PicoDsts")
    collHpssDuplicates = dbUtil.getCollection("HPSS_Duplicates")

    hpss = hpssUtil()
    hpss.setCollections(collHpssFiles, collHpssPicoDsts, collHpssDuplicates)
    hpss.getFileList()

    dbUtil.close()

# ____________________________________________________________________________
if __name__ == "__main__":
    print("Start HPSS Crawler!")
    sys.exit(main())
