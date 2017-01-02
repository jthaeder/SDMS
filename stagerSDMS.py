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
from subprocess import STDOUT, check_output

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
    def __init__(self, dbUtil, stageingFile):
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

        # -- Base folders for targets
        self._baseFolders = {'picoDst': 'picodsts',
                             'picoDstJet': 'picodsts/JetPicoDsts'}

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
        self._collsStageTargetNew = dict.fromkeys(self._listOfTargets)
        self._collsStageTargetMiss = dict.fromkeys(self._listOfTargets)

        for target in self._listOfTargets:
            self._collsStageTarget[target] = dict.fromkeys(self._listOfStageTargets)
            self._collsStageTargetNew[target] = dict.fromkeys(self._listOfStageTargets)
            self._collsStageTargetMiss[target] = dict.fromkeys(self._listOfStageTargets)

            for stageTarget in self._listOfStageTargets:
                self._collsStageTarget[target][stageTarget] = self._dbUtil.getCollection(stageTarget+'_'+ self._baseColl[target])
                self._collsStageTargetNew[target][stageTarget] = self._dbUtil.getCollection(stageTarget+'_'+ self._baseColl[target]+'_new')
                self._collsStageTargetMiss[target][stageTarget] = self._dbUtil.getCollection(stageTarget+'_'+ self._baseColl[target]+'_missing')

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
        # -s isntead -v
        self._stageXRD['xrdcpOptions'] = "-v --nopbar -S 4"

        nEntries = self._collServerXRD.find().count()
        self._stageXRD['tryMax'] = 2 * nEntries

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
                if hpssDocFile['filesInTar']*0.25 > len(hpssDocFile['listOfFiles']):
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
            isStagingSucessful = True

            # - Get next unstaged document and set status to staging
            stageDoc = collXRD.find_one_and_update({'stageStatusHPSS': 'staged', 'stageStatusTarget': 'unstaged'},
                                                   {'$set':{'stageStatusTarget': 'staging'}})
            if not stageDoc:
                break

            # -- Get stage server
            for serverTarget in stageDoc['stageTargetList']:

                # -- XRD command to copy from HPSS staging area to XRD
                xrdcpCmd = "xrdcp {0} {1}{2} xroot://{3}//star/{4}/{5}".format(self._stageXRD['xrdcpOptions'],
                    self._scratchSpace, stageDoc['fileFullPath'],
                    self._stageXRD['server'][serverTarget],
                    self._baseFolders[stageDoc['target']],
                    stageDoc['filePath'])
                cmd = shlex.split(xrdcpCmd)

                # -- Allow for several trials : 'tryMax'
                trial = 0
                while trial < self._stageXRD['tryMax']:
                    trial += 1
                    try:
                        output = check_output(cmd, stderr=STDOUT, timeout=self._stageXRD['timeOut'])

                    # -- Except error conditions
                    except subprocess.CalledProcessError as err:
                        isStagingSucessful = False
                        xrdCode = 'Unknown'

                        # -- Parse output for differnt XRD error conditions
                        for text in err.output.decode("utf-8").rstrip().split('\n'):
                            if "file already exists" in text:
                                xrdCode = "FileExistsAlready"
                                break
                            elif "no space left on device" in text:
                                xrdCode = "NoSpaceLeftOnDevice"
                                break

                        # -- File exits - not seeas as error
                        if xrdCode == 'FileExistsAlready':
                            isStagingSucessful = True
                            break

                        print("   Error XRD Staging: ({0}) {1}\n     {2}".format(err.returncode, xrdCode, err.cmd))
                        errorType = 'ErrorCode.{0}'.format(xrdCode)
                        collXRD.find_one_and_update({'fileFullPath': stageDoc['fileFullPath']},
                                                    {'$inc': {errorType: 1}, '$set': {'trials': trial}})

                        if xrdCode == 'Unknown':
                            note = err.output.decode("utf-8").rstrip()
                            collXRD.find_one_and_update({'fileFullPath': stageDoc['fileFullPath']}, {'$set': {'note': note}})
                            print(note)
                        else:
                            note = err.output.decode("utf-8").rstrip()
                            print(note)

                        continue

                    # -- Except timeout
                    except subprocess.TimeoutExpired as err:
                        isStagingSucessful = False
                        xrdCode = 'TimeOut'

                        print("   Error XRD Staging: ({0}) {1}\n     {2}".format(err.timeout, xrdCode, err.cmd))

                        errorType = 'ErrorCode_{0}'.format(xrdCode)
                        collXRD.find_one_and_update({'fileFullPath': stageDoc['fileFullPath']},
                                                    {'$inc': {errorType: 1}, '$set': {'trials': trial}})
                        continue

                    except:
                        isStagingSucessful = False
                        xrdCode = 'OtherError'

                        print("   Error XRD Staging: ({0}) {1}\n     {2}".format(xrdCode, xrdCode, xrdcpCmd))

                        errorType = 'ErrorCode_{0}'.format(xrdCode)
                        collXRD.find_one_and_update({'fileFullPath': stageDoc['fileFullPath']},
                                                    {'$inc': {errorType: 1}, '$set': {'trials': trial}})
                        continue

                    # -- XRD staging successful
                    if isStagingSucessful:
                        break

            # -- Clean up on disk and update collection
            if isStagingSucessful:

                # -- Remove file from disk
                fullFilePathOnScratch = self._scratchSpace + stageDoc['fileFullPath']
                try:
                    os.remove(fullFilePathOnScratch)
                except:
                    pass

                # -- Set status to staged
                collXRD.find_one_and_update({'fileFullPath': stageDoc['fileFullPath']},
                                            {'$set': {'stageStatusTarget': 'staged'}})

                # -- Tell servers have new files have been staged
                self._setFileHasBeenStagedToXRD()

            # -- Staging failed
            else:
                collXRD.find_one_and_update({'fileFullPath': stageDoc['fileFullPath']},
                                            {'$set': {'stageStatusTarget': 'failed'}})


    # ____________________________________________________________________________
    def _setFileHasBeenStagedToXRD(self):
        """Add that file as been added to XRD."""

        self._collServerXRD.update_many({'isDataServerXRD': True},
                                        {'$set' : {'newFilesStaged': True}})

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

        # -- Remove empty folders on scratch
        self._rmEmptyFoldersOnScratch()


    # ____________________________________________________________________________
    def _rmEmptyFoldersOnScratch(self):
        """Remove empty folders on scratch"""

        cmdLine = 'find ' + self._scratchSpace + '/project -type d -exec rmdir --ignore-fail-on-non-empty "{}" +'
        cmd = shlex.split(cmdLine)

        try:
            output = check_output(cmd, stderr=STDOUT)
        except:
            pass

    # ____________________________________________________________________________
    def printStagingStats(self):
        """Print staging statistics."""

        stageTarget = "XRD"
        target = 'picoDst'

        collXRD = self._collsStageToStageTarget[stageTarget]

        print(" All Docs in {}: {}".format(collXRD.name, collXRD.find().count()))
        print("   Unstaged: {}".format(collXRD.find({'stageStatusTarget': 'unstaged'}).count()))
        print("   Staged  : {}".format(collXRD.find({'stageStatusTarget': 'staged'}).count()))
        print("   Dummy   : {}".format(collXRD.find({'stageStatusHPSS': 'staged', 'stageDummy': True}).count()))
        print("   Failed  : {}".format(collXRD.find({'stageStatusTarget': 'failed'}).count()))
        print("   Staging : {}".format(collXRD.find({'stageStatusTarget': 'staging'}).count()))
        print(" ")
        print(" All Docs in {}: {}".format(self._collStageFromHPSS.name, self._collStageFromHPSS.find().count()))
        print("   Unstaged: {}".format(self._collStageFromHPSS.find({'stageStatus': 'unstaged'}).count()))
        print("   Staged  : {}".format(self._collStageFromHPSS.find({'stageStatus': 'staged'}).count()))
        print("   Failed  : {}".format(self._collStageFromHPSS.find({'stageStatus': 'failed'}).count()))
        print(" ")
        print(" Number of dataserver with new files staged (no new XRD Crawler Run: {}".format(self._collServerXRD.find({'isDataServerXRD': True, 'newFilesStaged': True}).count()))
        print(" Unprocessed new files on data server: {}".format(self._collsStageTargetNew[target][stageTarget].find().count()))
        print(" Unprocessed missing files on data server: {}".format(self._collsStageTargetMiss[target][stageTarget].find().count()))
        print(" ")
        print(" Free space on scratch disk: {} GB".format(self._getFreeSpaceOnScratchDisk()))
        print(" Used space in staging area: {} GB".format(self._getUsedSpaceOnStagingArea()))
        print(" ")
        print(" ")
        
    # ____________________________________________________________________________
    def checkForEndOfStagingCycle(self):
        """Check for end of staging cycle."""

        stageTarget = "XRD"
        target = 'picoDst'

        # -- XRD Crawler hasn't finshed everywhere
        if self._collServerXRD.find({'isDataServerXRD': True, 'newFilesStaged': True}).count() > 0:
            return

        # -- XRD Process hasn't finished
        if self._collsStageTargetNew[target][stageTarget].find().count() > 0 or self._collsStageTargetMiss[target][stageTarget].find().count() > 0:
            return

        # -- HPSS nothing left to stage
        if self._collStageFromHPSS.find({'stageStatus': 'unstaged'}).count() > 0:
            return

        # -- XRD nothing left to stage
        if collXRD.find({'stageStatusTarget': 'unstaged'}).count() > 0:
            return

        # -- End of cycle

        # - rm staged HPSS
        self._collStageFromHPSS.delete_many({'stageStatus': 'staged'})

        # - rm staged XRD
        collXRD.delete_many({'stageStatusTarget': 'staged'})

        # - end of staging cycle
        self._dbUtil.unsetProcessLock("staging_cycle_active")

        # -- Print staging stats
        self.printStagingStats()

# ____________________________________________________________________________
def main():
    """Initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

    stager = stagerSDMS(dbUtil, 'stagingRequest.json')

    # -- Prepare staging as start of a new staging cycle
    stager.prepareStaging()

    # -- Clean dummy staged files
    stager.cleanDummyStagedFiles()

    # -- Stage files from HPSS
    stager.stageHPSSFiles()

    stager.cleanDummyStagedFiles()

    # -- Stage from staging area to staging location
    stager.stageToXRD()

    # -- Check for end of staging cycle
    stager.checkForEndOfStagingCycle()

    dbUtil.close()

# ____________________________________________________________________________
if __name__ == "__main__":
    sys.exit(main())
