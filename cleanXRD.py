#!/usr/bin/env python
b'This script requires python 3.4'

"""
Script to clean XRD from broken links and corrupt files.

Process `XRD_<baseColl[target]>_corrupt`and `XRD_<baseColl[target]>_brokenLink`>.

For detailed documentation, see: README_XRD.md#xrd-process
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

XROOTD_BASE = '/export/data/xrd/ns'
XROOTD_SERVER = 'pstarxrdr1'

##############################################

# -- Check for a proper Python Version
if sys.version[0:3] < '3.0':
    print ('Python version 3.0 or greater required (found: {0}).'.format(sys.version[0:5]))
    sys.exit(-1)

# ----------------------------------------------------------------------------------
class cleanXRD:
    """Clean XRD of broken and corrupt files."""

    # _________________________________________________________
    def __init__(self, dbUtil):
        self._today = datetime.datetime.today().strftime('%Y-%m-%d')

        self._listOfTargets = ['picoDst', 'picoDstJet', 'aschmah']

        self._baseColl = {'picoDst': 'PicoDsts',
                          'picoDstJet': 'PicoDstsJets',
                          'aschmah': 'ASchmah'}

        self._addCollections(dbUtil)

    # _________________________________________________________
    def _addCollections(self, dbUtil):
        """Get collections from mongoDB."""

        self._collsHPSS = dict.fromkeys(self._listOfTargets)

        self._collsXRD = dict.fromkeys(self._listOfTargets)

        self._collsXRDCorrupt = dict.fromkeys(self._listOfTargets)
        self._collsXRDBrokenLink = dict.fromkeys(self._listOfTargets)

        for target in self._listOfTargets:
            self._collsHPSS[target] = dbUtil.getCollection('HPSS_' + self._baseColl[target])

            self._collsXRD[target] = dbUtil.getCollection('XRD_' + self._baseColl[target])

            self._collsXRDCorrupt[target] = dbUtil.getCollection('XRD_' + self._baseColl[target]+'_corrupt')
            self._collsXRDBrokenLink[target] = dbUtil.getCollection('XRD_' + self._baseColl[target]+'_brokenLink')

    # _________________________________________________________
    def processBrokenLinks(self, target):
        """process target"""

        print("Process Target:", target, "broken links")

        if target not in self._listOfTargets:
            print('Unknown "target"', target, 'for processing')
            return

        # - Get list of nodes with broken links
        nodeList = list(self._collsXRDBrokenLink[target].find().distinct('storage.detail'))

        # -- Get clean command for every node - to get only one ssh command per node
        for node in nodeList:

            pathList = list(item['fileFullPath'] for item in self._collsXRDBrokenLink[target].find({'storage.detail': node},
                                                                  {'fileFullPath': True, '_id': False}))

            fileListString = ' '.join(pathList)

            # -- On every node: remove files with broken links
            cmdLine = 'ssh {0} -oStrictHostKeyChecking=no -q "rm -f {1}"'.format(node, fileListString)
            cmd = shlex.split(cmdLine)
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

            for text in iter(p.stdout.readline, b''):
                print("Log: ", text.decode("utf-8").rstrip())

            # -- Clean up collection
            self._collsXRDBrokenLink[target].delete_many({'storage.detail': node})

    # _________________________________________________________
    def processCorrupt(self, target):
        """process corrupt files of target

            Loop over collection of corrupt files and remove them from XRD.
            if:
                - the fileSize is 0
                -
            """

        print("Process Target:", target, "corrupt")

        idxBasePath = len(XROOTD_BASE)+1

        # -- Delete all filese with size 0
        for doc in self._collsXRDCorrupt[target].find({'fileSize': 0}):
            fileFullPath = doc['fileFullPath']

            xrdCmd = "rm"

            # -- Remove files with fileSize of 0
            cmdLine = 'xrd {0} {1} {2}'.format(XROOTD_SERVER, xrdCmd, fileFullPath[idxBasePath:])
            cmd = shlex.split(cmdLine)
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

            errorCode = False
            logList = []
            for text in iter(p.stdout.readline, b''):
                logList.append(text.decode("utf-8").rstrip())
                if "The command returned an error." in text.decode("utf-8").rstrip():
                    errorCode = True

            # -- Clean up collection if no error occured
            if not errorCode:
                self._collsXRDCorrupt[target].delete_one({'_id': doc['_id']})
            else:
                print("Log: ", "Error deleteing", doc['storage']['detail'], ":", fileFullPath)
                for text in logList:
                    print("Log:   ", text)

        # -- Look at files sizes larger 0
        for doc in self._collsXRDCorrupt[target].find({'fileSize': {"$gt" : 0}}):
            fileFullPath = doc['fileFullPath']

            hpssDoc = self._collsHPSS[target].find_one({'filePath': doc['filePath'] })
            xrdDoc = self._collsRDX[target].find_one({'filePath': doc['filePath'] })

            print("XRD File Name: {0}".format(fileFullPath[idxBasePath:]))
            print("  Corrupt {0}".format(doc['fileSize'])
            print("  HPSS    {0}".format(hpssDoc['fileSize'])
            if (hpssDoc['isInTarFile']):
                print("    HPSS File Name:", hpssDoc['fileFullPathTar'])
                print("       File inside:", hpssDoc['fileFullPath'])
            else:
                print("    HPSS File Name:", hpssDoc['fileFullPath'])

            if xrdDoc:
                print("  XRD     {0}".format(xrdDoc['fileSize'])

# ____________________________________________________________________________
def main():
    """initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

    xrd = cleanXRD(dbUtil)

    # -- Process different targets
    target = 'picoDst'
    xrd.processBrokenLinks(target)
    xrd.processCorrupt(target)

    # -- Update data server DB
    dbUtil.close()

# ____________________________________________________________________________
if __name__ == "__main__":
    print("Start XRD Processing!")
    sys.exit(main())
