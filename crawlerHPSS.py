#!/usr/bin/env python
b'This script requires python 3.4'

"""
Do blab 

"""

import sys, os, re

import logging as log
import time
import socket
import datetime
import shlex, subprocess

from mongoUtil import mongoDbUtil 
import pymongo

from pymongo import results

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
    def __init__(self, dataClass = 'picoDst', pathKeysSchema = 'runyear/system/energy/trigger/production/day%d/runnumber%d'):
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

    # _________________________________________________________
    def _getFolderContent(self, picoFolder):
        """Get listing of content of picoFolder."""

        # -- Get subfolders from HPSS
        cmdLine = 'hsi -q ls -1 {0}/{1}'.format(HPSS_BASE_FOLDER, picoFolder)
        cmd = shlex.split(cmdLine)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
 
        # -- Loop of the list of subfolders
        for subFolder in iter(p.stdout.readline, b''):            
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
                                                                      {'$set': {'lastSeen': self._today}, '$setOnInsert' : doc}, 
                                                                      upsert = True)
                        
                        # -- record already there do nothing
                        if ret:
                            continue

                        # -- new record inserted - add the picoDst(s)
                        if doc['fileType'] == "picoDst":
                            listPicoDsts.append(self._makePicoDstDoc(doc['fileFullPath'], doc['fileSize'])) 
                        elif doc['fileType'] == "tar":
                            listPicoDsts += self._parseTarFile(doc)

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
        fileSize     = lineTokenized[4]
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

           return a list of picoDsts
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
            
            lineTokenized = lineCleaned.split(' ', 7)
            fileFullPath  = lineTokenized[6]
            fileSize      = lineTokenized[3]

            # -- select only dataClass
            if not fileFullPath.endswith(self._fileSuffix):
                continue
  
            # -- make PicoDst document and add it to list
            listDocs.append(self._makePicoDstDoc(fileFullPath, fileSize, hpssDoc=hpssDoc, isInTarFile=True))
            
        # --return list of picoDsts
        return listDocs

    # _________________________________________________________
    def _makePicoDstDoc(self, fileFullPath, fileSize, hpssDoc=None, isInTarFile=False): 
        """Create entry for picoDsts."""

        # -- identify start of "STAR naming conventions"
        idxBasePath = fileFullPath.find("/Run")+1

        # -- Create document
        doc = {'_id':          fileFullPath[idxBasePath:],
               'filePath':     fileFullPath[idxBasePath:],
               'fileFullPath': fileFullPath, 
               'fileSize':     fileSize,
               'dataClass':    self._dataClass,
               'isInTarFile':  isInTarFile,
               'staging':      { 'stageMarkerXRD': False}
            }            

        if isInTarFile:
            doc['fileFullPathTar'] = hpssDoc['fileFullPath']

        # -- Strip basePath of fileName and tokenize it 
        cleanPathTokenized = doc['filePath'].split(os.path.sep)

        # -- Create STAR details sub document
        docStarDetails = dict([(keys[0], self._typeMap[keys[1]](value)) 
                               for keys, value in zip(self._typedPathKeys, cleanPathTokenized)])

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

        print("Insert List: Try to add {0} picoDsts".format(len(listDocs)))

        # -- Insert list of picoDsts in to HpssPicoDsts collection
        ret = self._collHpssPicoDsts.insert_many(listDocs, ordered=False)
        
        # -- remove insertedIds from list of documents
        #    -> only duplicate are left
        for insertedId in ret.inserted_ids:
            element = next((item for item in listDocs if item['_id'] == insertedId), None)
            if element:
                listDocs.remove(element)

        # -- remove uniqueId '_id' in list of duplicates 
        for entry in listDocs:
            entry.pop('_id', None)
                    
        # -- Insert list of duplicate picoDsts in to HpssDuplicates collection
        if listDocs:
            print("Insert List: Found {0} duplicate picoDsts".format(len(listDocs)))
            self._collHpssDuplicates.insert_many(listDocs, ordered=False)


# ____________________________________________________________________________
def main():
    """initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

#    dbUtil.dropCollection("HPSS_Files")
#    dbUtil.dropCollection("HPSS_PicoDsts")
#    dbUtil.dropCollection("HPSS_Duplicates")

    collHpssFiles      = dbUtil.getCollection("HPSS_Files")
    collHpssPicoDsts   = dbUtil.getCollection("HPSS_PicoDsts")
    collHpssDuplicates = dbUtil.getCollection("HPSS_Duplicates")

    print(">>>", collHpssFiles)
    print("    Number of documents:", collHpssFiles.count())
    print(">>>", collHpssPicoDsts)
    print("    Number of documents:", collHpssPicoDsts.count())
    print(">>>", collHpssDuplicates)
    print("    Number of documents:", collHpssDuplicates.count())
   
    hpss = hpssUtil()
    hpss.setCollections(collHpssFiles, collHpssPicoDsts, collHpssDuplicates)
    hpss.getFileList()

    dbUtil.close()
# ____________________________________________________________________________
if __name__ == "__main__":
    print("Start HPSS Crawler!")
    sys.exit(main())
