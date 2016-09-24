#!/usr/bin/env python
b'This script requires python 3.4'

"""
bla


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
        self._collsXRDNoLink  = dict.fromkeys(self._listOfTargets)


        for target in self._listOfTargets:
            self._collsHPSS[target] = dbUtil.getCollection('HPSS_' + self._baseColl[target])

            self._collsXRD[target]        = dbUtil.getCollection('XRD_' + self._baseColl[target])
            self._collsXRDNew[target]     = dbUtil.getCollection('XRD_' + self._baseColl[target]+'_new')
            self._collsXRDMiss[target]    = dbUtil.getCollection('XRD_' + self._baseColl[target]+'_missing')
            self._collsXRDCorrupt[target] = dbUtil.getCollection('XRD_' + self._baseColl[target]+'_corrupt')
            self._collsXRDNoHPSS[target]  = dbUtil.getCollection('XRD_' + self._baseColl[target]+'_nohpss')
            self._collsXRDNoLink[target]  = dbUtil.getCollection('XRD_' + self._baseColl[target]+'_nolink')

    # _________________________________________________________
    def processNew(self, target):
        """process target"""

        print("Process Target:", target)

        if target not in self._listOfTargets:
            print('Unknown "target"', target, 'for processing')
            return

        # -- Loop over all documents in the new collection
        while True:

            # - Get first document of target
            xrdDocNew = self._collsXRDNew[target].find_one({'storage.location': 'XRD', 'target': target})
            if not xrdDocNew:
                break

            # -- Set of new nodes where file is stored at
            xrdDocs = list(self._collsXRDNew[target].find({'storage.location': 'XRD',
                                                           'target': target,
                                                           'filePath': xrdDocNew['filePath']}))

            nodeSet = set([item['storage']['detail'] for item in xrdDocs])

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
                detailsSet = set(existDoc['storage']['details'])
                detailsSet.update(nodeSet)

                # -- Check if the fileSizes match
                #    - if not move new document to extra collection : Corrupt
                if existDoc['fileSize'] != xrdDocNew['fileSize']:
                    self._collsXRDCorrupt[target].insert(xrdDocNew)
                    self._collsXRDNew[target].delete_one({xrdDocNew['_id']})
                    continue

                # -- Update existing document
                self._collsXRD[target].find_one_and_update({'storage.location': 'XRD', 'target': target,
                                                            'filePath': xrdDocNew['filePath']},
                                                           {'$set': {'storage.nCopies': len(detailsSet),
                                                                     'storage.details': list(detailsSet)}})

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
                   'storage' : {'location':'XRD', 'details': list(nodeSet), 'nCopies': len(nodeSet)}
                   }

            # -----------------------------------------------
            # -- Check if fileSizes are equal
            #    - if not move new document to extra collection : Corrupt
            #    - remove document from new collection
            #    - if all files in xrdDocs have the same size
            #      - just add all in collection
            #    - if not - only look at first document

            # -- Get all fileSizes of the same document
            fileSizeSet = set([item['fileSize'] for item in xrdDocs])

            # -- Check if fileSizes are equal
            if len(fileSizeSet) <= 1:

                # -- All are equal and equal to HPSS
                #    - insert document in collection
                #    - remove documents form new collection
                if hpssDoc['fileSize'] == xrdDocNew['fileSize']:
                    self._collsXRD[target].insert(doc)
                    self._collsXRDNew[target].delete_many({'storage.location': 'XRD',
                                                           'target': target,
                                                           'filePath': xrdDocNew['filePath']})
                    continue

                # -- All are equal and NOT equal to HPSS
                #    - move new documents to extra list
                #    - remove documents form new collection
                else:
                    self._collsXRDCorrupt[target].insert_many(xrdDocs)
                    self._collsXRDNew[target].delete_many({'storage.location': 'XRD',
                                                           'target': target,
                                                           'filePath': xrdDocNew['filePath']})
                    continue

            # -- Not all fileSizes are equal - only consider first document
            else:
                doc['storage']['details'] = xrdDocNew['storage']['detail']
                doc['storage']['nCopies'] = 1

                # -- Equal to HPSS
                #    - insert document in collection
                #    - remove documents form new collection
                if hpssDoc['fileSize'] == xrdDocNew['fileSize']:
                    self._collsXRD[target].insert(doc)
                    self._collsXRDNew[target].delete_one({xrdDocNew['_id']})
                    continue

                # -- Not equal to HPSS
                #    - move new documents to extra list
                #    - remove documents form new collection
                else:
                    self._collsXRDCorrupt[target].insert(xrdDocNew)
                    self._collsXRDNew[target].delete_one({xrdDocNew['_id']})
                    continue

    # _________________________________________________________
    def processMiss(self, target):
        """process target of missing files

            Loop over collection of missing files and remove them from
            XRD collection. If file has several copies, remove one copy.
            """

        print("Process Target:", target, "missing")

        if target not in self._listOfTargets:
            print('Unknown "target"', target, 'for processing')
            return

        # -- process broken links of target
        self._processMissBrokenLinks(target)

        # -- Loop over all documents in the new collection
        while True:

            # - Get first document of target
            xrdDocMiss = self._collsXRDMiss[target].find_one({'storage.location': 'XRD', 'target': target})
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
                print("Doc not even in list", xrdDocMiss['filePath'])
                self._collsXRDMiss[target].delete_one({xrdDocMiss['_id']})
                continue

            # -- Remove entry if only one copy
            if existDoc['storage']['nCopies'] == 1:
                self._collsXRD[target].delete_one({existDoc['_id']})

            # -- Remove one storge detail
            else:
                detailsSet = set(existDoc['storage']['details'])
                detailsSet.delete(xrdDocMiss['storage']['detail'])  ## CHECK THIS METHOD

                self._collsXRD[target].find_one_and_update({'storage.location': 'XRD',
                                                            'target': target,
                                                            'filePath': xrdDocMiss['filePath'],
                                                            'storage.details': xrdDocMiss['storage']['detail']},
                                                           {'$set': {'storage.nCopies': len(detailsSet),
                                                                     'storage.details': list(detailsSet)}})

            # -- Remove from list of missing
            self._collsXRDMiss[target].delete_one({xrdDocMiss['_id']})


    # _________________________________________________________
    def _processMissBrokenLinks(self, target):
        """Move broken links in new collection"""

        xrdDocs = self._collsXRDMiss[target].find({'storage.location': 'XRD',
                                                   'target': target,
                                                   'issue': 'brokenLink'})

        self._collsXRDNoLink[target].insert_many(xrdDocs)

        self._collsXRDMiss[target].delete_many({'storage.location': 'XRD',
                                                'target': target,
                                                'issue': 'brokenLink'})


# ____________________________________________________________________________
def main():
    """initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

    xrd = processXRD(dbUtil)

    # -- process different targets
    xrd.processNew('picoDst')
    xrd.processMiss('picoDst')

    # -- Update data server DB

    dbUtil.close()
# ____________________________________________________________________________
if __name__ == "__main__":
    print("Start XRD Processing!")
    sys.exit(main())
