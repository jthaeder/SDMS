#!/usr/bin/env python
b'This script requires python 3.4'

"""
Script to process the output of the `crawlerXRD.py` script.

Process `XRD_<baseColl[target]>_new`and `XRD_<baseColl[target]>_missing`
and update the `XRD_<baseColl[target]>` collections.

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

XROOTD_PREFIX = '/export/data/xrd/ns/star'
DISK_LIST = ['data', 'data1', 'data2', 'data3', 'data4']

##############################################

# -- Check for a proper Python Version
if sys.version[0:3] < '3.0':
    print ('Python version 3.0 or greater required (found: {0}).'.format(sys.version[0:5]))
    sys.exit(-1)

# ----------------------------------------------------------------------------------
class processXRD:
    """Process output of crawler scripts"""

    # _________________________________________________________
    def __init__(self, dbUtil):
        self._today = datetime.datetime.today().strftime('%Y-%m-%d')

        self._listOfTargets = ['picoDst', 'picoDstJet', 'aschmah']

        # -- base Collection Names
        self._baseStorage = ['XRD', 'HPSS']

        self._baseColl = {'picoDst': 'PicoDsts',
                          'picoDstJet': 'PicoDstsJets',
                          'aschmah': 'ASchmah'}

        self._addCollections(dbUtil)

    # _________________________________________________________
    def _addCollections(self, dbUtil):
        """Get collections from mongoDB."""

        self._collsHPSS = dict.fromkeys(self._listOfTargets)

        self._collsXRD        = dict.fromkeys(self._listOfTargets)
        self._collsXRDNew     = dict.fromkeys(self._listOfTargets)
        self._collsXRDMiss    = dict.fromkeys(self._listOfTargets)
        self._collsXRDCorrupt = dict.fromkeys(self._listOfTargets)
        self._collsXRDNoHPSS  = dict.fromkeys(self._listOfTargets)

        for target in self._listOfTargets:
            self._collsHPSS[target] = dbUtil.getCollection('HPSS_' + self._baseColl[target])

            self._collsXRD[target]        = dbUtil.getCollection('XRD_' + self._baseColl[target])
            self._collsXRDNew[target]     = dbUtil.getCollection('XRD_' + self._baseColl[target]+'_new')
            self._collsXRDMiss[target]    = dbUtil.getCollection('XRD_' + self._baseColl[target]+'_missing')
            self._collsXRDCorrupt[target] = dbUtil.getCollection('XRD_' + self._baseColl[target]+'_corrupt')
            self._collsXRDNoHPSS[target]  = dbUtil.getCollection('XRD_' + self._baseColl[target]+'_noHpss')

    # _________________________________________________________
    def processNew(self, target):
        """process target"""

        if target not in self._listOfTargets:
            print('Process Target: Unknown "target"', target, 'for processing')
            return

        # -- Loop over all documents in the new collection
        while True:

            # - Get first new document of target
            xrdDocNew = self._collsXRDNew[target].find_one({'storage.location': 'XRD', 'target': target})
            if not xrdDocNew:
                break

            # - Get all new documents of same file (filePath)
            xrdDocs = list(self._collsXRDNew[target].find({'storage.location': 'XRD',
                                                           'target': target,
                                                           'filePath': xrdDocNew['filePath']}))

            # -- Assure that all documents have the same fileSize.
            #    If not, use only the one file: xrdDocNew

            #    - Get all fileSizes of the same document
            fileSizeSet = set([item['fileSize'] for item in xrdDocs])

            #   - If fileSizes are not equal, reduce xrdDocs to xrdDocNew
            if len(fileSizeSet) > 1:
                xrdDocs = [xrdDocNew]

            # -- Set of new nodes where file (filePath) is stored at
            nodeSet = set([item['storage']['detail'] for item in xrdDocs])

            # -- Dictionary of detail : disk pairs
            nodeDiskDict = {item['storage']['detail']: item['storage']['disk'] for item in xrdDocs}

            # -----------------------------------------------
            # -- Check if there is an existing document
            #    - check if fileSizes are equal
            #      - if not move new document to extra list
            #      - remove document from new collection
            #    - update the the storage fields
            #    - remove document from new collection
            existDoc = self._collsXRD[target].find_one({'storage.location': 'XRD', 'target': target,
                                                        'filePath': xrdDocNew['filePath']})
            if existDoc:
                # -- Check if the fileSizes match
                #    - if not move new document to extra collection : Corrupt
                if existDoc['fileSize'] != xrdDocNew['fileSize']:
                    xrdDocNew['nodeFilePath'] = "{0}_{1}".format(xrdDocNew['storage']['detail'], xrdDocNew['filePath'])

                    try:
                        self._collsXRDCorrupt[target].insert(xrdDocNew)
                    except:
                        pass

                    self._collsXRDNew[target].delete_one({'_id': xrdDocNew['_id']})
                    continue

                # -- Update the set of all nodes where the file is stored
                detailsSet = set(existDoc['storage']['details'])
                detailsSet.update(nodeSet)

                newNodeDiskDict = existDoc['storage']['disks'].copy()
                newNodeDiskDict.update(nodeDiskDict)

                # -- Update existing document
                self._collsXRD[target].find_one_and_update({'storage.location': 'XRD', 'target': target,
                                                            'filePath': xrdDocNew['filePath']},
                                                           {'$set': {'storage.nCopies': len(detailsSet),
                                                                     'storage.details': list(detailsSet),
                                                                     'storage.disks': newNodeDiskDict}})

                # -- Remove entries from new collection
                self._collsXRDNew[target].delete_many({'storage.location': 'XRD', 'target': target,
                                                       'filePath': xrdDocNew['filePath']})

                continue

            # -----------------------------------------------
            # -- Get corresponding HPSS Document
            #    - if not move new document to extra collection
            #    - remove document from new collection
            #    - create new document
            hpssDoc = self._collsHPSS[target].find_one({'target': target,
                                                        'filePath': xrdDocNew['filePath'] })

            # -- Check if HPSS doc exists
            #    - if not move new document to extra collection : NoHPSS
            if not hpssDoc:
                xrdDocNew['nodeFilePath'] = "{0}_{1}".format(xrdDocNew['storage']['detail'], xrdDocNew['filePath'])

                try:
                    self._collsXRDNoHPSS[target].insert(xrdDocNew)
                except:
                    pass

                self._collsXRDNew[target].delete_one({'_id': xrdDocNew['_id']})
                continue

            # -- Create new document
            doc = {'starDetails': hpssDoc['starDetails'],
                   'target': hpssDoc['target'],
                   'fileSize': hpssDoc['fileSize'],
                   'filePath': hpssDoc['filePath'],
                   'fileFullPath': xrdDocNew['fileFullPath'],
                   'storage' : {
                        'location':'XRD',
                        'details': list(nodeSet),
                        'nCopies': len(nodeSet),
                        'disks': nodeDiskDict}
                   }

            # -----------------------------------------------
            # -- Check fileSizes of XRD entry/ies and HPSS entry
            #    - if equal: add all in collection
            #    - if not:  move new documents to extra collection : Corrupt
            if hpssDoc['fileSize'] == xrdDocNew['fileSize']:
                self._collsXRD[target].insert(doc)

            else:
                for item in xrdDocs:
                    item['nodeFilePath'] = "{0}_{1}".format(item['storage']['detail'], item['filePath'])

                try:
                    self._collsXRDCorrupt[target].insert_many(xrdDocs)
                except:
                    pass

            # -- Remove documents form new collection
            self._collsXRDNew[target].delete_many({'storage.location': 'XRD',
                                                   'target': target,
                                                   'filePath': xrdDocNew['filePath']})

    # _________________________________________________________
    def processMiss(self, target):
        """process target of missing files

            Loop over collection of missing files and remove them from
            XRD collection. If file has several copies, remove the copy of
            the node where its missing.
            """

        if target not in self._listOfTargets:
            print('Process Target: Unknown "target"', target, 'for processing')
            return

        # -- Loop over all documents in the new collection
        while True:

            # - Get first document of target
            xrdDocMiss = self._collsXRDMiss[target].find_one({'storage.location': 'XRD',
                                                              'target': target,
                                                              'issue':'missing'})
            if not xrdDocMiss:
                break

            # -----------------------------------------------
            # -- Get existing document
            existDoc = self._collsXRD[target].find_one({'storage.location': 'XRD', 'target': target,
                                                        'filePath': xrdDocMiss['filePath'],
                                                        'storage.details': xrdDocMiss['storage']['detail']})
            # -- Not a document
            #    - remove it from list dependend on cases
            if not existDoc:
                print("Process Target: Doc not even in list", xrdDocMiss['filePath'])
                self._collsXRDMiss[target].delete_one({'_id': xrdDocMiss['_id']})
                continue

            # -- Remove entry if only one copy
            if existDoc['storage']['nCopies'] == 1:
                self._collsXRD[target].delete_one({'_id': existDoc['_id']})

            # -- Remove one storage detail
            else:
                detailsSet = set(existDoc['storage']['details'])
                detailsSet.discard(xrdDocMiss['storage']['detail'])

                self._collsXRD[target].find_one_and_update({'storage.location': 'XRD',
                                                            'target': target,
                                                            'filePath': xrdDocMiss['filePath'],
                                                            'storage.details': xrdDocMiss['storage']['detail']},
                                                           {'$set': {'storage.nCopies': len(detailsSet),
                                                                     'storage.details': list(detailsSet)}})

            # -- Remove from list of missing
            self._collsXRDMiss[target].delete_one({'_id': xrdDocMiss['_id']})

# ____________________________________________________________________________
def main():
    """initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

    xrd = processXRD(dbUtil)

    # -- Process different targets
    #    - Prevent from running when another process job is running.
    #    - Make sure the crawlers are not running when processing is ongoing
    target = 'picoDst'
    if not dbUtil.checkSetProcessLock("process_XRD_{0}".format(target)):
        xrd.processMiss(target)
        xrd.processNew(target)
        dbUtil.unsetProcessLock("process_XRD_{0}".format(target))

    # -- Update data server DB
    dbUtil.close()

# ____________________________________________________________________________
if __name__ == "__main__":

    sys.exit(main())
