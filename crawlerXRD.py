#!/usr/bin/env python
b'This script requires python 3.4'

"""
Crawler which runs daily on all data server nodes and inspects the XRD space.
Findings are stored in mongoDB collections.

For detailed documentation, see: README_XRD.md#xrd-crawler
"""

import sys
import os
import os.path
import re
import json
import shutil
import psutil

import logging as log
import time
import socket
import datetime
import shlex, subprocess
import errno

from mongoUtil import mongoDbUtil
import pymongo

from pymongo import results
from pymongo import errors
from pymongo import bulk

from pprint import pprint

##############################################
# -- GLOBAL CONSTANTS

XROOTD_PREFIX = '/export/data/xrd/ns/star'
DISK_LIST = ['data', 'data1', 'data2', 'data3', 'data4']

##############################################

# -- Check for a proper Python Version
if sys.version[0:3] < '3.0':
    print ('Python version 3.0 or greater required (found: {0}).'.format(sys.version[0:5]))
    sys.exit(-1)

# ----------------------------------------------------------------------------------
class crawlerXRD:
    """Runs on storage node and compares filelists"""

    # _________________________________________________________
    def __init__(self, dbUtil):
        self._now = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
        self._nodeName = socket.getfqdn().split('.')[0]

        # -- possible targets
        self._listOfTargets = ['picoDst', 'picoDstJet', 'aschmah']

        # -- Base folders for targets
        self._baseFolders = {'picoDst': 'picodsts',
                             'picoDstJet': 'picodsts/JetPicoDsts',
                             'aschmah': 'picodsts/aschmah'}

        # -- Base collection names for targets
        self._baseColl = {'picoDst': 'PicoDsts',
                          'picoDstJet': 'PicoDstsJets',
                          'aschmah': 'ASchmah'}

        self._dbUtil = dbUtil
        self._addCollections()

    # _________________________________________________________
    def _addCollections(self):
        """Get collections from mongoDB."""

        self._colls       = dict.fromkeys(self._listOfTargets)
        self._collsNew    = dict.fromkeys(self._listOfTargets)
        self._collsMiss   = dict.fromkeys(self._listOfTargets)
        self._collsNoLink = dict.fromkeys(self._listOfTargets)

        for target in self._listOfTargets:
            self._colls[target]       = self._dbUtil.getCollection('XRD_' + self._baseColl[target])
            self._collsNew[target]    = self._dbUtil.getCollection('XRD_' + self._baseColl[target]+'_new')
            self._collsMiss[target]   = self._dbUtil.getCollection('XRD_' + self._baseColl[target]+'_missing')
            self._collsNoLink[target] = self._dbUtil.getCollection('XRD_' + self._baseColl[target]+'_brokenLink')

        self._collDataServer = self._dbUtil.getCollection("XRD_DataServers")

    # _________________________________________________________
    def process(self, target):
        """process target"""

        # print("Process Target:", target, "on", self._nodeName)

        if target not in self._listOfTargets:
            print('Unknown "target"', target, 'for processing')
            return

        # -- Check for ongoing processing of last crawler run
        if self._dbUtil.checkProcessLock("process_XRD_{0}".format(target)):
            print ("Postponed - XRD {0} data is currently processed".format(target))
            return

        # -- Get list of files stored on this node
        listOfFilesOnNode = list(item['filePath']
                                 for item in self._colls[target].find({'target': target,
                                                                       'storage.location': 'XRD',
                                                                       'storage.details': self._nodeName},
                                                                      {'filePath': True, '_id': False}))

        # -- Check if server is not a dataserver
        if self._collDataServer.find({'nodeName': self._nodeName, 'isDataServerXRD': True}).count() == 0:
            # -- Add files to DB as missing - if there are some
            if listOfFilesOnNode:
                self._insertMissingFilePaths(listOfFilesOnNode, target)
            return

        # -- Get working directoty
        self._workDir = os.path.join(XROOTD_PREFIX, self._baseFolders[target])

        # -- Check if working directory exists
        if not os.path.isdir(self._workDir):
            # -- Add missing files to DB - if there are some
            if listOfFilesOnNode:
                self._insertMissingFilePaths(listOfFilesOnNode, target)
            return

        # -- Get list folders to walk on
        ignoreList = [os.path.join(XROOTD_PREFIX, value) for key, value in self._baseFolders.items()
                      if key != target ]

        folderList = [name for name in os.listdir(self._workDir)
                      if os.path.isdir(os.path.join(self._workDir, name))
                      and os.path.join(self._workDir, name) not in ignoreList]

        listOfNewFiles = []

        # -- Run over folders for target
        for folder in folderList:
            for root, dirs, files in os.walk(os.path.join(self._workDir, folder)):
                for fileName in files:

                    # -- document of current file
                    doc = {'fileFullPath': os.path.join(root, fileName),
                           'filePath': os.path.join(root[len(self._workDir)+1:], fileName),
                           'storage': {'location': 'XRD',
                                       'detail': self._nodeName,
                                       'disk': ''},
                           'target': target,
                           'fileSize': -1}

                    # -- check if file link is ok and get size
                    try:
                        fstat = os.stat(doc['fileFullPath'])
                    except OSError as e:
                        doc['issue'] = 'brokenLink'
                        doc['nodeFilePath'] = '{0}_{1}'.format(self._nodeName, doc['filePath'])
                        try:
                            self._collsNoLink[target].insert(doc)
                        except:
                            pass
                        continue

                    doc['fileSize'] = fstat.st_size
                    doc['storage']['disk'] = os.readlink(doc['fileFullPath']).split('/')[2]

                    # -- If fileName in listOfFilesOnNode
                    #    -> Do Nothing
                    if doc['filePath'] in listOfFilesOnNode:
                        listOfFilesOnNode.remove(doc['filePath'])
                        continue

                    # -- New file add to list of files to be added
                    listOfNewFiles.append(doc)

        # -- Add new files to DB
        if listOfNewFiles:
            self._collsNew[target].insert_many(listOfNewFiles, ordered=False)

        # -- Add missing files to DB
        if listOfFilesOnNode:
            self._insertMissingFilePaths(listOfFilesOnNode, target)

    # _________________________________________________________
    def _insertMissingFilePaths(self, filePathList, target):
        """insert list of filePaths to missing collection"""

        for filePath in filePathList:
            doc = {'fileFullPath': "",
                'filePath': filePath,
                'storage': {'location': 'XRD',
                            'detail': self._nodeName,
                            'disk': ''},
                'target': target,
                'fileSize': -1,
                'issue': 'missing'}

            self._collsMiss[target].insert(doc)

    # _________________________________________________________
    def updateServerInfo(self):
        """update info Server"""

        # -- Set of mounted data partitions
        mountSet = set(disk.mountpoint for disk in psutil.disk_partitions()
                       if '/export/data' in disk.mountpoint)

        # -- Get disk usage
        total=0
        used=0
        free=0

        for diskPath in mountSet:
            if os.path.isdir(diskPath):
                usage = shutil.disk_usage(diskPath)

                used += usage.used
                total += usage.total
                free += usage.free

        # -- update DB
        doc = {'nodeName': self._nodeName,
               'stateActive': False,
               'lastSeenActive': '2000-01-01-01-01',}

        self._collDataServer.find_one_and_update({'nodeName': doc['nodeName']},
                                                 {'$set': {'freeSpace': free,
                                                           'usedSpace': used,
                                                           'totalSpace': total,
                                                           'newFilesStaged': False,
                                                           'lastCrawlerRun': self._now},
                                                  '$setOnInsert' : doc}, upsert = True)

# ____________________________________________________________________________
def main():
    """initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

    xrd = crawlerXRD(dbUtil)

    # -- Process different targets
    xrd.process('picoDst')
#    xrd.process('picoDstJet')
#    xrd.process('aschmah')

    # -- Update data server DB
    xrd.updateServerInfo()

    dbUtil.close()
# ____________________________________________________________________________
if __name__ == "__main__":
    #print("Start XRD Crawler on", socket.getfqdn().split('.')[0])
    sys.exit(main())
