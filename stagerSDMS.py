#!/usr/bin/env python
b'This script requires python 3.4'

"""
Script to prepare staging of files according to stageing file.

Stager which reads staging file, sets stageMarker in `HPSS_<Target>` collection
and prepares staging to target.

For detailed documentation, see: README_StageXRD.md
"""

import sys
import os
import re
import json

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

SCRATCH_SPACE = "/global/projecta/projectdirs/starprod/stageArea"
SCRATCH_LIMIT = 10*1024*1024*1024*1024

HPSS_TAPE_ORDER_SCRIPT ="/usr/common/usg/bin/hpss_file_sorter.script"

##############################################

# -- Check for a proper Python Version
if sys.version[0:3] < '3.0':
    print ('Python version 3.0 or greater required (found: {0}).'.format(sys.version[0:5]))
    sys.exit(-1)

# ----------------------------------------------------------------------------------
class stagerSDMS:
    """ Stager to from HPSS at NERSC"""

    # _________________________________________________________
    def __init__(self, dbUtil, stageingFile, scratchSpace):
        self._stageingFile = stageingFile
        self._scratchSpace = SCRATCH_SPACE
        self._scratchLimit = SCRATCH_LIMIT

        self._listOfStageTargets = ['XRD']  # , 'Disk']

        self._listOfQueryItems   = ['runyear', 'system', 'energy',
                                    'trigger', 'production', 'day',
                                    'runnumber', 'stream']

        self._listOfTargets = ['picoDst', 'picoDstJet']

        # -- base Collection Names
        self._baseColl = {'picoDst': 'PicoDsts',
                          'picoDstJet': 'PicoDstsJets'}

        self._readStagingFile()

        self._addCollections(dbUtil)

    # _________________________________________________________
    def _readStagingFile(self):
        """Read in staging file."""

        with open(self._stageingFile) as dataFile:
            setList = json.load(dataFile)

            try:
                self._sets = setList['sets']
            except:
                print('Error reading staging file: no "sets" found')
                sys.exit(-1)

            try:
                self._nCopies = setList['nCopies']
            except:
                self._nCopies = 1

    # _________________________________________________________
    def _addCollections(self, dbUtil):
        """Get collections from mongoDB."""

        # -- Data server collection
        self._collServerXRD = dbUtil.getCollection('XRD_DataServers')

        # -- HPPS files collection
        self._collsHPSSFiles = dbUtil.getCollection('HPSS_Files')

        # -- Collections in HPSS - the 'truth'
        self._collsHPSS = dict.fromkeys(self._listOfTargets)
        for target in self._listOfTargets:
            self._collsHPSS[target] = dbUtil.getCollection('HPSS_' + self._baseColl[target])

        # -- Collections for the staging target
        self._collsStageTarget = dict.fromkeys(self._listOfTargets)
        for target in self._listOfTargets:
            self._collsStageTarget[target] = dict.fromkeys(self._listOfStageTargets)

            for stageTarget in self._listOfStageTargets:
                self._collsStageTarget[target][stageTarget] = dbUtil.getCollection(stageTarget+'_'+ self._baseColl[target])
                foo = stageTarget+'_'+ self._baseColl[target]
                print("COLL: ", target, stageTarget, foo)

        # -- Collection of files to stage from HPSS
        self._collStageFromHPSS = dbUtil.getCollection('Stage_From_HPSS')

        # -- Collection of files to stage to stageTarget
        self._collsStageToStageTarget = dict.fromkeys(self._listOfStageTargets)
        for stageTarget in self._listOfStageTargets:
            self._collsStageToStageTarget[stageTarget] = dbUtil.getCollection('Stage_To_'+stageTarget)

    # _________________________________________________________
    def markFilesToBeStaged(self):
        """Mark files to be staged from staging file in `HPSS_<target>`."""

        self.numberOfFilesToBeStaged()

        # -- Reset previous stage markers
        self._resetAllStagingMarks()

        self.numberOfFilesToBeStaged()

        # -- Loop over every set from staging file one-by-one as stageSet
        for stageSet in self._sets:
            if not self._prepareSet(stageSet):
                continue

            # -- Set stage marker using the the stageSet as find query
            self._collsHPSS[self._target].update_many(stageSet, {'$set': {self._targetField: True}})

    # _________________________________________________________
    def _resetAllStagingMarks(self):
        """Reset all staging marks in `HPSS_<target>`."""

        # -- Rest all staging markers
        for target, collection in self._collsHPSS.items():
            for targetKey in set().union(*(dic.keys() for dic in collection.distinct('staging'))):
                targetField = "staging.{0}".format(targetKey)
                collection.update_many({}, {'$set': {targetField: False}})

    # _________________________________________________________
    def numberOfFilesToBeStaged(self):
        """Prints number of all files to be staged `HPSS_<target>`."""

        for target, collection in self._collsHPSS.items():
            for targetKey in set().union(*(dic.keys() for dic in collection.distinct('staging'))):
                targetField = "staging.{0}".format(targetKey)
                nStaged = collection.find({targetField: True}).count()

                print('For {0} in collection: {1}'.format(target, collection.name))
                print('   Files to be staged with {0}: {1}'.format(targetField, nStaged))

    # _________________________________________________________
    def _prepareSet(self, stageSet):
        """Prepare set to be staged.

            Do basic checks, returns False if set can't be staged
        """

        # -- Check for stageTarget
        try:
            stageTarget = stageSet['stageTarget']
            if stageTarget not in  self._listOfStageTargets:
                print('Error reading staging file: Unknown "stageTarget"', stageTarget)
                self._stageTarget = None
                return False
            self._targetField = "staging.stageMarker{0}".format(stageTarget)

        except:
            print('Error reading staging file: no "stageTarget" found in set' , stageSet)
            self._stageTarget = None
            return False

        # -- Check for target
        try:
            target = stageSet['target']
            if target not in  self._listOfTargets:
                print('Error reading staging file: Unknown "target"', target)
                self._target = None
                return False
            self._target = target

        except:
            print('Error reading staging file: no "target" found in set' , stageSet)
            self._target = None
            return False

        # -- Clean up
        del(stageSet['target'])
        del(stageSet['stageTarget'])

        # -- Check if query items are correct
        for key, value in stageSet.items():
            if "starDetails." in key:
                continue
            if key not in self._listOfQueryItems:
                print('Error reading staging file: Query item does not exist:', key, value)
                return False
            del(stageSet[key])
            starKey = 'starDetails.' + key
            stageSet[starKey] = value

        return True

    # _________________________________________________________
    def getListOfFilesFromHPSS(self):
        """Get list of files of type target to be retrieved from HPSS"""

        # -- Loop over targets (picoDsts, etc.)
        for target in self._listOfTargets:

            # -- Loop over stage targets (XRD, disk)
            for stageTarget in self._listOfStageTargets:
                stageField = 'staging.stageMarker{0}'.format(stageTarget)

                # -- Get all documents from HPSS to be staged
                hpssDocs = list(self._collsHPSS[target].find({'target': target, stageField: True}))
                docsSetHPSS = set([item['filePath'] for item in hpssDocs])

                # -- Get all files on stageing Target
                stagedDocs = list(self._collsStageTarget[target][stageTarget].find({'storage.location': stageTarget,
                                                                                'target': target}))
                docsSetStaged = set([item['filePath'] for item in stagedDocs])

                print("hpss   ", len(docsSetHPSS))
                print("xrd    ", len(docsSetStaged))

                # -- Document to be staged
                docsToStage = docsSetHPSS - docsSetStaged

                # -- Documents to be removed from stageTarget
                docsToRemove = docsSetStaged - docsSetHPSS

                print("xrd    ", len(docsSetStaged))
                print("xrdnew ", len(docsToStage))
                print("xrdrm  ", len(docsToRemove))

                # -- Get collection to stage from HPSS and to stageTarget
                self._prepareStageColls(docsToStage, target, stageTarget)




            # -- Mark Documents as to be unStaged
#            self._collsStageTarget[target][stageTarget].update_many({'filePath' : '$in' : docsToRemove},
#                                                              { '$set': {'unStageFlag': True} })


            # -- Make list of documents to be removed
 #           mark collection files in staged collection as  to be removed
  #          -> use other clear script to explictly remove


            #            get files  which are not in tar file
            #                -> make list of files to be stage
            #                    - get files from HPSS to staging area
            #- use hpss tape ordering


            #get list file in tar balls  -> sort by tar file name
            #            -> disti   nct -> via set

#    loop over tarballs and get nFiles per tar ball
#    if nFiles is larger then 25%
#        (get all files from Tarball into stageingArea)
#        add tar ball to stage list -> (use hpss tapeordering on it)






    #  ____________________________________________________________________________
    def _prepareStageColls(self, docsToStage, target, stageTarget):
        """Fill collection of files to stage.

           - Fill collection to stage from HPSS to Disk
           - Fill collection to stage from Disk to Target
           """

        print("PREPARE : ", target, stageTarget)

        # -- Loop over all paths and gather information
        for currentPath in docsToStage:

            hpssDoc = self._collsHPSS[target].find_one({'filePath': currentPath})

            if hpssDoc['isInTarFile']:
                hpssFilePath = hpssDoc['fileFullPathTar']
            else:
                hpssFilePath = hpssDoc['fileFullPath']

            # -- Get doc of actual file on HPSS
            hpssDocFile = self._collsHPSSFiles.find_one({'fileFullPath': hpssFilePath})

            # -- Create doc : stageDocFromHPSS
            stageDocFromHPSS = {'fileFullPath': hpssFilePath,
                                'stageStatus': 'unstaged'}

            if hpssDoc['isInTarFile']:
                stageDocFromHPSS['filesInTar'] = hpssDocFile['filesInTar']
                stageDocFromHPSS['isInTarFile'] = True

            # -- Update doc in collStageFromHPSS if doc exists otherwise add new
            self._collStageFromHPSS.find_one_and_update({'fileFullPath': hpssFilePath},
                                                        {'$inc' : {'nDocs':1},
                                                         '$addToSet': {'listOfFiles': hpssDoc['fileFullPath']},
                                                         '$setOnInsert' : stageDocFromHPSS}, upsert = True)

            # -- Get Update doc in collStageToStagingTarget
            stageDocToTarget = {
                'filePath':     currentPath,
                'fileFullPath': hpssDoc['fileFullPath'],
                'fileSize':     hpssDoc['fileSize'],
                'target':       hpssDoc['target'],
                'nCopiesExist': 0,
                'nCopiesMissing': self._nCopies}

            # -- Get nCopies from stageTarget collection
            xrdDoc = self._collsStageTarget[target][stageTarget].find_one({'filePath': currentPath})
            if (xrdDoc):
                nCopiesExist = xrdDoc['storage']['nCopies']
                nCopiesMissing = self._nCopies - xrdDoc['storage']['nCopies']


            # -- Insert new doc in collStageToStagingTarget
            try:
                self._collsStageToStageTarget[stageTarget].insert(stageDocToTarget)
            except:
                pass


    #  ____________________________________________________________________________
    def _createTapeOrderingHPSS(self):
        """Create tape ordering"""

        with open("{0}/orderMe.txt".format(SCRATCH_SPACE), "w") as orderMe:
            for hpssDocFile in self._collStageFromHPSS.find({'stageStatus':'unstaged'}):
                print(hpssDocFile[fileFullPath], file=orderMe)

        # -- Call tape ordering script
        cmdLine = '{0} {1}/orderMe.txt'.format(HPSS_TAPE_ORDER_SCRIPT, SCRATCH_SPACE)
        cmd = shlex.split(cmdLine)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        # -- Process output and update collection
        orderIdx = 0
        for text in iter(p.stdout.readline, b''):
            print(text, orderIdx)
            ++orderIdx

            self._collStageFromHPSS.find_one_and_update({'fileFullPath': text,'stageStatus': 'unstaged'},
                                                        {'$set' :{ 'orderIdx': orderIdx}}


    #  ____________________________________________________________________________
    def stageHPSSFiles(self):
        """Stage list of files from HPSS on to scratch space"""

        # -- Get tape ordering
        self._createTapeOrderingHPSS()

        return

        listOfFilesToStage = []

        ## -- Decide on to stage file or subFile
        for hpssDocFile in self._collsHPSSFiles.find({'stageStatus':'unstaged'}):
            if not hpssDocFile['isInTarFile']:
                listOfFilesToStage.append()


        #-> tape ordering
        #-> stage files ->


    #  ____________________________________________________________________________
    def _checkScratchSpaceStatus(self, updateValue):
        """Check free space on disk.

           Inital OS check and recheck from time to time.
           Otherwise use update of fileSize.
        """


    #  ____________________________________________________________________________
    def _totalSize(source):
        """Get Total size of source folder."""

        totalSize = os.path.getsize(source)
        for item in os.listdir(source):
            itempath = os.path.join(source, item)
            if os.path.isfile(itempath):
                totalSize += os.path.getsize(itempath)
            elif os.path.isdir(itempath):
                totalSize += self._totalSize(itempath)
        return totalSize



    # ____________________________________________________________________________
    def stage(self):
        """Stage all files from stageing area to staging location"""

# ____________________________________________________________________________
def main():
    """Initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

    stager = stagerSDMS(dbUtil, 'stagingRequest.json', os.getenv('SCRATCH', SCRATCH_SPACE))

    # -- Mark files to be staged
    stager.markFilesToBeStaged()
    stager.numberOfFilesToBeStaged()

    # -- Get list of files to be staged
    stager.getListOfFilesFromHPSS()

    # -- Stage files from HPSS
    stager.stageHPSSFiles()

    # -- Stage from staging area to staging location
#    stager.stage()

    dbUtil.close()
# ____________________________________________________________________________
if __name__ == "__main__":
    print("Start SDMS Stager!")
    sys.exit(main())
