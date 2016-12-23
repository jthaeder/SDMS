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
SCRATCH_LIMIT = 10*1024 # in GB

HPSS_TAPE_ORDER_SCRIPT ="/usr/common/usg/bin/hpss_file_sorter.script"

META_MANAGER = "pstarxrdr1"

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

        self._dbUtil = dbUtil

        self._scratchSpaceFlag = True  # still enough space

        self._listOfStageTargets = ['XRD']  # , 'Disk']

        self._listOfQueryItems   = ['runyear', 'system', 'energy',
                                    'trigger', 'production', 'day',
                                    'runnumber', 'stream']

        self._listOfTargets = ['picoDst', 'picoDstJet']

        # -- base Collection Names
        self._baseColl = {'picoDst': 'PicoDsts',
                          'picoDstJet': 'PicoDstsJets'}

        # -- Get collections
        self._addCollections()

        # -- Get XRD staging parameters
        self._getXRDStagingParameters()

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
    def _addCollections(self):
        """Get collections from mongoDB."""

        # -- Data server collection
        self._collServerXRD = self._dbUtil.getCollection('XRD_DataServers')

        # -- HPPS files collection
        self._collsHPSSFiles = self._dbUtil.getCollection('HPSS_Files')

        # -- Collections in HPSS - the 'truth'
        self._collsHPSS = dict.fromkeys(self._listOfTargets)
        for target in self._listOfTargets:
            self._collsHPSS[target] = self._dbUtil.getCollection('HPSS_' + self._baseColl[target])

        # -- Collections for the staging target
        self._collsStageTarget = dict.fromkeys(self._listOfTargets)
        for target in self._listOfTargets:
            self._collsStageTarget[target] = dict.fromkeys(self._listOfStageTargets)

            for stageTarget in self._listOfStageTargets:
                self._collsStageTarget[target][stageTarget] = self._dbUtil.getCollection(stageTarget+'_'+ self._baseColl[target])

        # -- Collection of files to stage from HPSS
        self._collStageFromHPSS = self._dbUtil.getCollection('Stage_From_HPSS')

        # -- Collection of files to stage to stageTarget
        self._collsStageToStageTarget = dict.fromkeys(self._listOfStageTargets)
        for stageTarget in self._listOfStageTargets:
            self._collsStageToStageTarget[stageTarget] = self._dbUtil.getCollection('Stage_To_'+stageTarget)

    # _________________________________________________________
    def _getXRDStagingParameters(self):
        """Get staging parameters for XRD"""

        self._stageXRD = dict()

        self._stageXRD['timeOut'] = 1800
        self._stageXRD['xrdcpOptions'] = "-v -np -S 4"

        nEntries = self._collServerXRD.find().count()
        self._stageXRD['tryMax'] = 10 * nEntries

        self._stageXRD['server'] = dict()
        doc = self._collServerXRD.find_one({'roles':'MENDEL_ONE_MANAGER'})
        self._stageXRD['server']['MENDEL_1'] = doc['nodeName'] + '-ib.nersc.gov'

        doc = self._collServerXRD.find_one({'roles':'MENDEL_TWO_MANAGER'})
        self._stageXRD['server']['MENDEL_2'] = doc['nodeName'] + '-ib.nersc.gov'

        doc = self._collServerXRD.find_one({'roles':'META_MANAGER'})
        if doc:
            self._stageXRD['server']['MENDEL_ALL'] = doc['nodeName'] + '.nersc.gov'
        else:
            self._stageXRD['server']['MENDEL_ALL'] = META_MANAGER + '.nersc.gov'

    # _________________________________________________________
    def prepareStaging(self):
        """Perpare staging as start of a new cycle"""

        # -- start new staging cycle
        if not self._dbUtil.checkSetProcessLock("staging_cycle_active"):

            # -- Read in staging File
            self._readStagingFile()

            # -- Mark files to be staged in HPSS list
            self.markFilesToBeStaged()
            self.numberOfFilesToBeStaged()

            # -- Get list of files to be staged
            self.getListOfFilesFromHPSS()

            # -- Get tape ordering for HPSS files
            self.createTapeOrderingHPSS()

    # _________________________________________________________
    def markFilesToBeStaged(self):
        """Mark files to be staged from staging file in `HPSS_<target>`."""

        # -- Reset previous stage markers
        self._resetAllStagingMarks()

        # -- Loop over every set from staging file one-by-one as stageSet
        for stageSet in self._sets:
            if not self._prepareSet(stageSet):
                continue

            # -- Set stage marker using the the stageSet as find query
            self._collsHPSS[self._target].update_many(stageSet, {'$set': {self._targetField: True}})

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
                                'stageStatus': 'unstaged',
                                'target': target,
                                'stageTarget': stageTarget}

            if hpssDoc['isInTarFile']:
                stageDocFromHPSS['filesInTar'] = hpssDocFile['filesInTar']
                stageDocFromHPSS['isInTarFile'] = True

            # -- Update doc in collStageFromHPSS if doc exists otherwise add new
            self._collStageFromHPSS.find_one_and_update({'fileFullPath': hpssFilePath},
                                                        {'$addToSet': {'listOfFiles': hpssDoc['fileFullPath']},
                                                         '$setOnInsert' : stageDocFromHPSS}, upsert = True)

            # -- Get Update doc in collStageToStagingTarget
            stageDocToTarget = {
                'filePath':     currentPath,
                'fileFullPath': hpssDoc['fileFullPath'],
                'fileSize':     hpssDoc['fileSize'],
                'target':       hpssDoc['target'],
                'stageStatusHPSS': 'unstaged',
                'stageStatusTarget': 'unstaged'}

            # -- Basic set of stage targets if no document exists
            if self._nCopies == 1:
                stageDocToTarget['stageTargetList'] = ['MENDEL_ALL']
            else:
                stageDocToTarget['stageTargetList'] = ['MENDEL_1', 'MENDEL_2']

            # -- Get list of sub cluster for stageTarget collection if document exists already
            xrdDoc = self._collsStageTarget[target][stageTarget].find_one({'filePath': currentPath})
            if (xrdDoc):

                stageTargetList = []
                for node in xrdDoc['storage']['details']:
                    nodeDoc = self._collServerXRD.find_one({'nodeName': node})
                    if not 'MENDEL_ONE_DATASERVER' in nodeDoc['roles']:
                        stageTargetList.append('MENDEL_1')
                    if not 'MENDEL_TWO_DATASERVER' in nodeDoc['roles']:
                        stageTargetList.append('MENDEL_2')

                if len(stageTargetList) > 0:
                    stageDocToTarget[stageTargetList] = stageTargetList

            # -- Insert new doc in collStageToStagingTarget
            try:
                self._collsStageToStageTarget[stageTarget].insert(stageDocToTarget)
            except:
                pass

    #  ____________________________________________________________________________
    def createTapeOrderingHPSS(self):
        """Create tape ordering"""

        # -- Write order file
        with open("{0}/orderMe.txt".format(self._scratchSpace), "w") as orderMe:
            for hpssDocFile in self._collStageFromHPSS.find({'stageStatus':'unstaged'}):
                print(hpssDocFile['fileFullPath'], file=orderMe)

        # -- Call tape ordering script
        cmdLine = '{0} {1}/orderMe.txt'.format(HPSS_TAPE_ORDER_SCRIPT, self._scratchSpace)
        cmd = shlex.split(cmdLine)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        # -- Process output and update collection
        orderIdx = 0
        for text in iter(p.stdout.readline, b''):
            self._collStageFromHPSS.find_one_and_update({'fileFullPath': text.decode("utf-8").rstrip(), 'stageStatus': 'unstaged'},
                                                        {'$set' : {'orderIdx': orderIdx}})
            orderIdx += 1

        # -- Clean up order file
        os.remove("{0}/orderMe.txt".format(self._scratchSpace))

    #  ____________________________________________________________________________
    def stageHPSSFiles(self):
        """Stage list of files from HPSS on to scratch space

           Implemented as single squential process to keep file ordering.
        """

        if self._dbUtil.checkSetProcessLock("stagingHPSS"):
            return

        listOfFilesToStage = []

        ## -- Decide on to stage file or subFile
        for hpssDocFile in self._collStageFromHPSS.find({'stageStatus':'unstaged'}).sort('orderIdx', pymongo.ASCENDING):

            # -- Check if there is enough space on disk
            if not self._checkScratchSpaceStatus():
                break

            # -- Use hsi to extract one file only
            if not hpssDocFile['isInTarFile']:
                self._extractHPSSFile(hpssDocFile['fileFullPath'], hpssDocFile['stageTarget'])

            # -- Use htar to extract from a htar file
            else:
                extractFileWise = False

                # -- more the 25% percent of all file need to be extracted -> Get the whole file
                if hpssDocFile['filesInTar']*0.25 < len(hpssDocFile['listOfFiles']):
                    extractFileWise = True

                self._extractHPSSTarFile(hpssDocFile['fileFullPath'], hpssDocFile['stageTarget'],
                                         hpssDocFile['listOfFiles'], hpssDocFile['target'], extractFileWise)

        self._dbUtil.unsetProcessLock("stagingHPSS")

    #  ____________________________________________________________________________
    def _checkScratchSpaceStatus(self):
        """Check free space on disk.

           Returs true if still enough space to stage more from HPSS
        """

        if not self._scratchSpaceFlag:
            return False

        freeSpace = self._getFreeSpaceOnScratchDisk()
        usedSpace = self._getUsedSpaceOnStagingArea()

        # -- Check that freespace is larger 1 TB, otherwise set flag to false
        if freeSpace < 1024:
            self._scratchSpaceFlag = False

        # -- Check that not more than the limit is used
        if usedSpace > self._scratchLimit:
            self._scratchSpaceFlag = False

        return True

    # ____________________________________________________________________________
    def _getFreeSpaceOnScratchDisk(self):
        """Get free space on disk in GB."""

        # -- Call tape ordering script
        cmdLine = '/usr/common/usg/bin/prjaquota starprod'
        cmd = shlex.split(cmdLine)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        for text in iter(p.stdout.readline, b''):
            if 'starprod' in text.decode("utf-8").rstrip():
                line = text.decode("utf-8").rstrip().split()
                freeSpace = int(line[2]) - int(line[1]) #  in GB  all - used
                break

        return freeSpace

    # ____________________________________________________________________________
    def _getUsedSpaceOnStagingArea(self):
        """Get used space on stageing area in GB."""

        pipe = [{'$match': {'stageStatusHPSS': 'staged'}},
	        {'$group': {'_id': None, 'total': {'$sum': '$fileSize'}}}]

        usedSpace = 0
        for stageTarget in self._listOfStageTargets:
            for res in self._collsStageToStageTarget[stageTarget].aggregate(pipeline=pipe):
                if res:
                    usedSpace += res['total']

        return int(usedSpace / 1073741824.) # Bytes in GBytes

    # ____________________________________________________________________________
    def _extractHPSSFile(self, fileFullPath, stageTarget):
        """Extract one file only using hsi."""

        print("NOT IMPEMENTED YET")
        #        self._collStageFromHPSS.find_one_and_update({'fileFullPath': fileFullPath)},
        #                                                                {'$set' : {'stageStatus': 'staged'}})

    # ____________________________________________________________________________
    def _extractHPSSTarFile(self, fileFullPath, stageTarget, listOfFiles, target, extractFileWise):
        """Extract from HTAR files using htar."""

        # -- Extract file-by-file and add it to staging target collection as staged
        if extractFileWise:
            isExctractSucessful = True

            for targetFile in listOfFiles:
                # -- htar of single file
                cmdLine = 'htar -xf {0} {1}'.format(fileFullPath, targetFile)
                cmd = shlex.split(cmdLine)
                p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, cwd=self._scratchSpace)

                isFileExctractSucessful = False
                for lineTerminated in iter(p.stdout.readline, b''):
                    line = lineTerminated.decode("utf-8").rstrip('\t\n')
                    lineCleaned = ' '.join(line.split())

                    if lineCleaned == "HTAR: HTAR SUCCESSFUL":
                        isfileExctractSucessful = True
                        break

                isExctractSucessful += isfileExctractSucessful

                # -- Update staging traget if extraction was successful
                if isfileExctractSucessful:
                    self._collsStageToStageTarget[stageTarget].find_one_and_update({'fileFullPath': targetFile},
                                                                                   {'$set': {'stageStatusHPSS': 'staged'}})
        # -- Extract the full file
        else:
            isExctractSucessful = False

            # -- htar of full file
            cmdLine = 'htar -xf {0}'.format(fileFullPath)
            cmd = shlex.split(cmdLine)
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, cwd=self._scratchSpace)

            for lineTerminated in iter(p.stdout.readline, b''):
                line = lineTerminated.decode("utf-8").rstrip('\t\n')
                lineCleaned = ' '.join(line.split())

                if lineCleaned == "HTAR: HTAR SUCCESSFUL":
                    isExctractSucessful = True
                    break

            if isExctractSucessful:
                for doc in self._collsHPSS[target].find({'fileFullPathTar': fileFullPath}):

                    upsertDoc = {'fileFullPath': doc['fileFullPath'],
                                'fileSize': doc['fileSize'],
                                'stageDummy': True}
                    
                    self._collsStageToStageTarget[stageTarget].find_one_and_update({'fileFullPath': doc['fileFullPath']},
                                                                                   {'$set': {'stageStatusHPSS': 'staged'},
                                                                                    '$setOnInsert': upsertDoc}, upsert = True)

        # -- Update stage status of HPSS stage collection
        stageStatus = 'staged' if isExctractSucessful else 'failed'
        self._collStageFromHPSS.find_one_and_update({'fileFullPath': fileFullPath},
                                                    {'$set' : {'stageStatus': stageStatus}})


    # ____________________________________________________________________________
    def stageToXRD(self):
        """Stage all files from stageing area to staging target."""

        # -- Remove dummy files from full file stage
        self.cleanDummyStagedFiles()

        stageTarget = "XRD"
        collXRD = self._collsStageToStageTarget[stageTarget]

        # -- Loop over all documents in target collection
        while True:

            # - Get next unstaged document and set status to staging
            try:
                stageDoc = collXRD.find_one_and_update({'stageStatusHPSS': 'staged', 'stageStatusTarget': 'unstaged'},
                                                       {'$set':{'stageStatusTarget': 'staging'}})
            except:
                break

            if not stageDoc:
                break



            """
            - XRD stage file
	           - once or twice / depending on the targets
	             - set retry count to twice the number of nodes in XRD_DataServer
                 - if done delete file from disk
                 - set doc to XRDstaged and HPSSremoved and set fileSize to 0
            """

    # ____________________________________________________________________________
    def cleanDummyStagedFiles(self):
        """Remove all dummy staged files"""

        for stageTarget in self._listOfStageTargets:
            collTarget = self._collsStageToStageTarget[stageTarget]

            fileListToDelete = [doc['fileFullPath'] for doc in collTarget.find({'stageStatusHPSS': 'staged', 'stageDummy': True})]
            for fileName in fileListToDelete:
                dummyFile = self._scratchSpace + fileName
                try:
                     os.remove(dummyFile)
                except:
                    pass

                try:
                    collTarget.delete_one({'fileFullPath': fileName})
                except:
                    pass

    # ____________________________________________________________________________
    def checkForEndOfStagingCycle(self):
        """Check for end of staging cycle."""

        pass
#  -> unset lock when all files in HPSSstage are done or failed
#  (print out failed)



# ____________________________________________________________________________
def main():
    """Initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

    stager = stagerSDMS(dbUtil, 'stagingRequest.json', os.getenv('SCRATCH', SCRATCH_SPACE))

    # -- Prepare staging as start of a new staging cycle
    stager.prepareStaging()

    # -- Clean dummy staged files
    stager.cleanDummyStagedFiles()

    # -- Stage files from HPSS
    stager.stageHPSSFiles()

    stager.cleanDummyStagedFiles()

    # -- Stage from staging area to staging location
#    stager.stageToXRD()

    dbUtil.close()
# ____________________________________________________________________________
if __name__ == "__main__":
    print("Start SDMS Stager!")
    sys.exit(main())
