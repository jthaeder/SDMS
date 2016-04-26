#!/usr/bin/env python
b'This script requires python 3.4'

"""
Do blab 

"""

import sys
import os

import logging as log
import time
import socket
import datetime
import shlex, subprocess

from mongoUtil import mongoDbUtil 

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
    def __init__(self):
        self._today = datetime.datetime.today().strftime('%Y-%m-%d')

    # _________________________________________________________
    def setCollections(self, collHpssFiles, collHpssPicoDsts):
        """Get collection from mongoDB"""
        
        self._collHpssFiles    = collHpssFiles
        self._collHpssPicoDsts = collHpssPicoDsts

    # _________________________________________________________
    def getFileList(self):
        """Loop over both folders containing picoDSTs on HPSS"""

        for picoFolder in PICO_FOLDERS:
            print("LL", picoFolder)
            self._getFolderContent(picoFolder)
            break

    # _________________________________________________________
    def _getFolderContent(self, picoFolder):
        """get listing of content of picoFolder"""

        # -- Get subfolders from HPSS
        cmdLine = 'hsi -q ls -1 {0}/{1}'.format(HPSS_BASE_FOLDER, picoFolder)
        cmd = shlex.split(cmdLine)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        count = 0
 
        # -- Loop of the list of subfolders
        for subFolder in iter(p.stdout.readline, b''):            
            print("subs", subFolder.decode("utf-8").rstrip())
            lists = self._parseSubFolder(subFolder.decode("utf-8").rstrip())

            for entry in lists:
                print("xx", entry)



            count += 1
            if count >1:
                break
            
    # _________________________________________________________
    def _parseSubFolder(self, subFolder): 
        """Get recursive list of folders and files in subFolder ... as "ls" output"""
        
        cmdLine = 'hsi -q ls -lR {0}'.format(subFolder)
        cmd = shlex.split(cmdLine)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        listDocs = []

        # -- Parse ls output line-by-line -> utilizing output blocks in ls
        inBlock = 0        
        lcount = 0
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
                        ret = self._collHpssFiles.find_one_and_update({'fileFullName': doc['fileFullName']}, 
                                                                      {'$set': {'lastSeen': self._today}, '$setOnInsert' : doc}, 
                                                                      upsert = True)
                        # -- new record inserted
                        if ret:
#                        if not ret:
                            if doc['fileType'] == "picoDst":
                                picoDstDoc = self._makePicoDstDoc(doc) 
                                print("              no record 1")
                            elif doc['fileType'] == "tar":
                                self._parseTarFile(doc)
                                print("              no record 2")

                        listDocs.append(doc)


                        lcount += 1
                        if lcount > 4:
                            break

                        
        return listDocs

    # _________________________________________________________
    def _parseLine(self, line): 
        """Parse one entry in HPSS subfolder
           Get every file with full path, size, and details"""

        lineTokenized = line.split(' ', 9)
        
        fileName     = lineTokenized[8]
        fileFullName = "{0}/{1}".format(self._currentBlockPath, fileName)
        fileSize     = lineTokenized[4]
        fileType     = "other"
        
        if fileName.endswith(".tar"):
            fileType = "tar"
        elif fileName.endswith(".idx"):
            fileType = "idx"
        elif fileName.endswith(".picoDst.root"):
            fileType = "picoDst"
            
        # -- make record
        doc = { "fileFullName": fileFullName, "fileSize": fileSize, "fileType": fileType}

        return doc

    # _________________________________________________________
    def _parseTarFile(self, hpssDoc):
        """Get Content of tar file and parse it"""

        cmdLine = 'htar -tf {0}'.format(hpssDoc['fileFullName'])
        cmd = shlex.split(cmdLine)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        for lineTerminated in iter(p.stdout.readline, b''):
            line = lineTerminated.decode("utf-8").rstrip('\t\n')
            lineCleaned = ' '.join(line.split())
            
            if lineCleaned == "HTAR: HTAR SUCCESSFUL":
                continue
            
            # -- pick only root files 
            if not lineCleaned.startswith('HTAR: d'):  
                lineTokenized = lineCleaned.split(' ', 7)

                fileName     = lineTokenized[6]
                fileSize     = lineTokenized[3]
            
                if not fileName.endswith('.root'):
                    print(lineTokenized)
                    continue

                if fileName.endswith(".picoDst.root"):
                    fileType = "picoDst"


                doc = { "fileNamePicoDst": fileName, "fileSize": fileSize, "fileType": fileType, "isInTarFile": True, "fileNameTarFile": hpssDoc['fileFullName']}
                
                print(doc)


                        
    # _________________________________________________________
    def _makePicoDstDoc(self, hpssDoc): 
        """Create entry for picoDsts """




# ____________________________________________________________________________
def main():
    """initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")
    collHpssFiles    = dbUtil.getCollection("HPSS_Files")
    collHpssPicoDsts = dbUtil.getCollection("HPSS_PicoDsts")

    print("connected")


    print(">>>", collHpssFiles)
    print(">>>", collHpssPicoDsts)

    for doc in collHpssFiles.find({}):
        print("    >> ", doc) 

   
    hpss = hpssUtil()
    hpss.setCollections(collHpssFiles, collHpssPicoDsts)
    hpss.getFileList()

    dbUtil.close()
# ____________________________________________________________________________
if __name__ == "__main__":
    print("xx")
    sys.exit(main())
