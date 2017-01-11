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
import psutil

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

HPSS_TAPE_ORDER_SCRIPT = "/usr/common/usg/bin/hpss_file_sorter.script"
HPSS_SPLIT_MAX = 10

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

        # -- Get HPSS staging parameters
        self._getHPSSStagingParameters()

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

        # -- Process Locks
        self._collProcessLocks = self._dbUtil.getCollection('Process_Locks')

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
    def _getHPSSStagingParameters(self):
        """Get staging parameters for HPSS"""

        self._stageHPSS = dict()

        self._stageHPSS['tapeOrderScript'] = HPSS_TAPE_ORDER_SCRIPT

        self._stageHPSS['splitMax'] = HPSS_SPLIT_MAX

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

                # -- Document to be staged
                docsToStage = docsSetHPSS - docsSetStaged

                # -- Documents to be removed from stageTarget
                docsToRemove = docsSetStaged - docsSetHPSS

                # -- Get collection to stage from HPSS and to stageTarget
                self._prepareStageColls(docsToStage, target, stageTarget)

    #  ____________________________________________________________________________
    def _prepareStageColls(self, docsToStage, target, stageTarget):
        """Fill collection of files to stage.

           - Fill collection to stage from HPSS to Disk
           - Fill collection to stage from Disk to Target
           """

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
            emptyList = []
            stageDocToTarget = {
                'filePath':     currentPath,
                'fileFullPath': hpssDoc['fileFullPath'],
                'fileSize':     hpssDoc['fileSize'],
                'target':       hpssDoc['target'],
                'stageStatusHPSS': 'unstaged',
                'stageStatusTarget': 'unstaged',
                'note': emptyList}

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
        cmdLine = '{0} {1}/orderMe.txt'.format(self._stageHPSS['tapeOrderScript'], self._scratchSpace)
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

        # -- Split stage set
        self._splitStageFromHPSS()

    #  ____________________________________________________________________________
    def _splitStageFromHPSS(self):
        """Split list in n parts"""

        nAll = self._collStageFromHPSS.find({'stageStatus':'unstaged'}).count()

        if nAll <= self._stageHPSS['splitMax']:
            self._collStageFromHPSS.update_many({'stageStatus':'unstaged'}, {'$set' : {'stageGroup': 1}})
            return

        self._collStageFromHPSS.update_many({'stageStatus':'unstaged'}, {'$set' : {'stageGroup': -1}})

        split = int(nAll / self._stageHPSS['splitMax'])

        for splitIdx in range(1, self._stageHPSS['splitMax']+2):
            for doc in self._collStageFromHPSS.find({'stageStatus':'unstaged',
                                                     'stageGroup': -1}, {'_id':True}).sort('orderIdx', pymongo.ASCENDING).limit(split):

                self._collStageFromHPSS.update_one({'_id': doc['_id']}, {'$set': {'stageGroup': splitIdx}})

    #  ____________________________________________________________________________
    def stageFromHPSS(self):
        """Stage list of files from HPSS on to scratch space

           Implemented as single squential process to keep file ordering.
        """

        lock = True
        for stageGroup in sorted(self._collStageFromHPSS.distinct('stageGroup')):
            if not self._dbUtil.checkSetProcessLock("stagingHPSS_{}".format(stageGroup)):
                 lock = False
                 break

        if lock:
            return

        listOfFilesToStage = []

        ## -- Decide on to stage file or subFile
        while True:
            now = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')

            # - Get next unstaged document and set status to staging
            stageDoc = self._collStageFromHPSS.find_one_and_update({'stageStatus': 'unstaged', 'stageGroup': stageGroup},
                                                                   {'$set':{'stageStatus': 'staging',
                                                                    'timeStamp': now}}, sort=[('orderIdx', pymongo.ASCENDING)])
            if not stageDoc:
                break

            # -- Check if there is enough space on disk
            if not self._checkScratchSpaceStatus():
                break

            # -- Use hsi to extract one file only
            if not stageDoc['isInTarFile']:
                self._extractHPSSFile(stageDoc['fileFullPath'], stageDoc['stageTarget'])

            # -- Use htar to extract from a htar file
            else:
                extractFileWise = False

                # -- more the 25% percent of all file need to be extracted -> Get the whole file
                if stageDoc['filesInTar']*0.25 > len(stageDoc['listOfFiles']):
                    extractFileWise = True

                self._extractHPSSTarFile(stageDoc['fileFullPath'], stageDoc['stageTarget'],
                                         stageDoc['listOfFiles'], stageDoc['target'], extractFileWise)

        self._dbUtil.unsetProcessLock("stagingHPSS_{}".format(stageGroup))

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

        pipe = [{'$match': {'stageStatusHPSS': 'staged', 'stageStatusTarget': { '$ne': "staged" } }},
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
            now = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')

            # - Get next unstaged document and set status to staging
            stageDoc = collXRD.find_one_and_update({'stageStatusHPSS': 'staged', 'stageStatusTarget': 'unstaged'},
                                                   {'$set':{'stageStatusTarget': 'staging', 'timeStamp':now}})
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
                            elif "No such file or directory" in text:
                                xrdCode = "NoSuchFileOrDirectory"
                                break

                        # -- File exits - not seeas as error
                        if xrdCode == 'FileExistsAlready':
                            isStagingSucessful = True
                            break

                        # -- Update entry
                        errorType = 'ErrorCode_{0}'.format(xrdCode)
                        note = err.output.decode("utf-8").rstrip()
                        collXRD.find_one_and_update({'fileFullPath': stageDoc['fileFullPath']},
                                                    {'$inc': {errorType: 1},
                                                     '$set': {'trials': trial},
                                                     '$push': {'note': note}})
                        continue

                    # -- Except timeout
                    except subprocess.TimeoutExpired as err:
                        isStagingSucessful = False
                        xrdCode = 'TimeOut'

                        errorType = 'ErrorCode_{0}'.format(xrdCode)
                        note = "Time out after {0}s".format(self._stageXRD['timeOut'])
                        collXRD.find_one_and_update({'fileFullPath': stageDoc['fileFullPath']},
                                                    {'$inc': {errorType: 1},
                                                     '$set': {'trials': trial},
                                                     '$push': {'note': note}})
                        continue

                    except:
                        isStagingSucessful = False
                        xrdCode = 'OtherError'

                        errorType = 'ErrorCode_{0}'.format(xrdCode)
                        note = "Other error"
                        collXRD.find_one_and_update({'fileFullPath': stageDoc['fileFullPath']},
                                                    {'$inc': {errorType: 1},
                                                     '$set': {'trials': trial},
                                                     '$push': {'note': note}})
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

    # ____________________________________________________________________________
    def _rmEmptyFoldersOnScratch(self):
        """Remove empty folders on scratch"""

        open('{}/.hiddenMarker'.format(self._scratchSpace),'a').close()
        for root, dirs, files in os.walk(self._scratchSpace, topdown=False):
            if len(files) == 0 and len(dirs) == 0:
                os.removedirs(root)

    # ____________________________________________________________________________
    def printStagingStats(self):
        """Print staging statistics."""

        stageTarget = "XRD"
        target = 'picoDst'

        collXRD = self._collsStageToStageTarget[stageTarget]

        if collXRD.find().count() > 0:
            print(" All Docs in {}: {}".format(collXRD.name, collXRD.find().count()))
            print("   Unstaged   : {}".format(collXRD.find({'stageStatusTarget': 'unstaged'}).count()))
            print("   Unstaged   : {} but staged already from HPSS".format(collXRD.find({'stageStatusTarget': 'unstaged', 'stageStatusHPSS': 'staged'}).count()))
            print("   Staged     : {}".format(collXRD.find({'stageStatusTarget': 'staged'}).count()))
            print("   Dummy      : {}".format(collXRD.find({'stageStatusHPSS': 'staged', 'stageDummy': True}).count()))
            print("   Failed     : {}".format(collXRD.find({'stageStatusTarget': 'failed'}).count()))
            print("   Investigate: {}".format(collXRD.find({'stageStatusTarget': 'investigate'}).count()))
            print("   Staging    : {}".format(collXRD.find({'stageStatusTarget': 'staging'}).count()))
            print(" ")
        if self._collStageFromHPSS.find().count() > 0:
            print(" All Docs in {}: {}".format(self._collStageFromHPSS.name, self._collStageFromHPSS.find().count()))
            print("   Unstaged: {}".format(self._collStageFromHPSS.find({'stageStatus': 'unstaged'}).count()))
            print("   Staged  : {}".format(self._collStageFromHPSS.find({'stageStatus': 'staged'}).count()))
            print("   Staging : {}".format(self._collStageFromHPSS.find({'stageStatus': 'staging'}).count()))
            print("   Failed  : {}".format(self._collStageFromHPSS.find({'stageStatus': 'failed'}).count()))
            print(" ")
        if self._collServerXRD.find({'isDataServerXRD': True, 'newFilesStaged': True}).count() > 0:
            print(" Number of dataserver with new files staged: {} (no new XRD Crawler Run)".format(self._collServerXRD.find({'isDataServerXRD': True, 'newFilesStaged': True}).count()))
            print(" ")
        if self._collsStageTargetNew[target][stageTarget].find().count() > 0:
            print(" Unprocessed new files on data server:       {}".format(self._collsStageTargetNew[target][stageTarget].find().count()))
            print(" ")
        if self._collsStageTargetMiss[target][stageTarget].find().count() > 0:
            print(" Unprocessed missing files on data server:   {}".format(self._collsStageTargetMiss[target][stageTarget].find().count()))
            print(" ")
        print(" Free space on scratch disk: {} GB".format(self._getFreeSpaceOnScratchDisk()))
        print(" Used space in staging area: {} GB".format(self._getUsedSpaceOnStagingArea()))
        print(" ")
        print(" ")

    # ____________________________________________________________________________
    def _checkUnstaged(self):
        """Check if no unstaged are left - return False if none left"""

        # -- HPSS nothing left to stage
        if self._collHPSS.find({'stageStatus': 'unstaged'}).count() > 0:
            return True

        # -- XRD nothing left to stage
        if self._collXRD.find({'stageStatusTarget': 'unstaged'}).count() > 0:
            return True

        return False

    # ____________________________________________________________________________
    def _checkFailed(self):
        """Check if some failed files are left"""

        # -- Loop over all failed Target files
        for doc in self._collXRD.find({'stageStatusTarget': 'failed'}):

            # -- If files have been rested more the 10 times, set them to investigate
            resetFailed = 0
            try:
                resetFailed = doc['resetFailed']
            except:
                pass

            if resetFailed > 10:
                self._collXRD.update_one({'_id': doc['_id']},
                                         {'$set': {'stageStatusTarget': 'investigate'}})

            # -- Get other errors than no space left on device
            otherError = []
            hasNoSpace = False
            hasNoFile = False
            for key, value in doc.items():
                if "ErrorCode" in key:
                    if "NoSpaceLeftOnDevice" in key
                        hasNoSpace = True
                    elif "NoSuchFileOrDirectory" in key:
                        hasNoFile = True
                    else:
                        otherError.append(key)

            # -- If file is gone from staging area (why so ever) delete entry
            if hasNoFile:
                self._collXRD.delete_one({'_id': doc['_id']})
                continue

            # -- If all errors are no space left on device, reset to unstaged and increase resetFailed count
            if hasNoSpace and len(otherError) == 0:
                self._collXRD.update_one({'_id': doc['_id']},
                                         {'$set': {'stageStatusTarget': 'unstaged'},
                                          '$inc': {'resetFailed': 1}})

            # -- Else , set to investigate
            self._collXRD.update_one({'_id': doc['_id']}, {'$set': {'stageStatusTarget': 'investigate'}})

    # ____________________________________________________________________________
    def _checkStaging(self):
        """Check if files are still staging

           set them back to unstaged in case of too long in staging
        """

        # -- Set all files from HPSS - still in staging state after 8 hours back to unstaged
        nHoursAgo = (datetime.datetime.now() - datetime.timedelta(hours=8)).strftime('%Y-%m-%d-%H-%M')
        self._collHPSS.update_many({'stageStatusTarget': 'staging', 'timeStamp': {"$lt": nHoursAgo}},
                                   {'$set': {'stageStatus': 'unstaged'}})

        # -- Set all files to XRD - still in staging state after 4 hours back to unstaged
        nHoursAgo = (datetime.datetime.now() - datetime.timedelta(hours=4)).strftime('%Y-%m-%d-%H-%M')
        self._collXRD.update_many({'stageStatusTarget': 'staging', 'timeStamp': {"$lt": nHoursAgo}},
                                  {'$set': {'stageStatusTarget': 'unstaged'},
                                   '$inc': {'resetFailed': 1}})

    # ____________________________________________________________________________
    def killZombieXRDCP(self):
        """Kill zombie xrdcp processes if not in staging cycle."""

        if not self._dbUtil.checkProcessLock("staging_cycle_active"):
            xrdcpCmd = "xrdcp"

            for proc in psutil.process_iter():
                if proc.name() == xrdcpCmd:
                    try:
                        proc.kill()
                    except:
                        pass

    # ____________________________________________________________________________
    def checkForEndOfStagingCycle(self):
        """Check for end of staging cycle."""

        stageTarget = "XRD"
        target = 'picoDst'

        self._collHPSS = self._collStageFromHPSS
        self._collXRD  = self._collsStageToStageTarget[stageTarget]

        # -- Clean up staged
        # --------------------------------------------

        # - rm staged HPSS
        self._collHPSS.delete_many({'stageStatus': 'staged'})

        # - rm staged XRD
        self._collXRD.delete_many({'stageStatusTarget': 'staged'})

        # -- Check that no are none unstaged and no staging or failed are left
        # --------------------------------------------
        if self._checkUnstaged():
            return

        # -- Check that none are failed
        self._checkFailed()

        # -- Check what to do with files in staging
        self._checkStaging()

        # -- check again if some files have been reset
        if self._checkUnstaged():
            return

        # -- Check for the crawler to be finished and outcome has been processed
        # --------------------------------------------

        # -- XRD Crawler hasn't finshed everywhere
        if self._collServerXRD.find({'isDataServerXRD': True, 'newFilesStaged': True}).count() > 0:
            return

        # -- XRD Process hasn't finished
        if self._collsStageTargetNew[target][stageTarget].find().count() > 0 or self._collsStageTargetMiss[target][stageTarget].find().count() > 0:
            return

        # -- End of cycle
        # --------------------------------------------

        # - end of staging cycle
        self._dbUtil.unsetProcessLock("staging_cycle_active")

        # -- remove stageing locks
        for key in self._collProcessLocks.find_one({'unique':'unique'}).keys():
            if 'stagingHPSS' in key:
                self._collProcessLocks.find_one_and_update({'unique':'unique'}, {'$unset': {key: 1}})

        # -- Print staging stats
        self.printStagingStats()

        # -- Remove empty folders on scratch
        self._rmEmptyFoldersOnScratch()

        # -- Kill zombie xrdcp processes
        self.killZombieXRDCP()

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
    stager.stageFromHPSS()

    # -- Clean dummy staged files
    stager.cleanDummyStagedFiles()

    # -- Stage from staging area to staging location
    stager.stageToXRD()

    # -- Check for end of staging cycle
    stager.checkForEndOfStagingCycle()

    dbUtil.close()

# ____________________________________________________________________________
if __name__ == "__main__":
    sys.exit(main())
