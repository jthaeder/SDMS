#!/usr/bin/env python
b'This script requires python 3.4'

"""
Stager which reads staging file and sets stageMarker in HPSSPicoDsts collection

The staging file can have several sets, 

each set can have the following parameters

 'target'   : 'XRD'           (For now the only option)
    as in listOfTargets = ['XRD']

 'dataClass': 'picoDst'       (For now the only option)
    as in  listOfDataClasses = ['picoDst']

 Data set parameters:
    as in listOfQueryItems = ['runyear', 'system', 'energy', 'trigger', 'production', 'day', 'runnumber', 'stream']

 with example values:
 'runyear': 'Run10', 
 'system': 'AuAu', 
 'energy': '11GeV', 
 'trigger': 'all', 
 'production': 'P10ih', 
 'day': 149, 
 'runnumber': 11149081, 
 'stream': 'st_physics_adc',
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


##############################################

# -- Check for a proper Python Version
if sys.version[0:3] < '3.0':
    print ('Python version 3.0 or greater required (found: {0}).'.format(sys.version[0:5]))
    sys.exit(-1)

# ----------------------------------------------------------------------------------
class stagerSDMS:
    """ Stager to from HPSS at NERSC""" 

    # _________________________________________________________
    def __init__(self, stageingFile):
        self._stageingFile = stageingFile
        self._listOfTargets = ['XRD']
        self._listOfDataClasses = ['picoDst']
        self._listOfQueryItems = ['runyear', 'system', 'energy', 'trigger', 'production', 'day', 'runnumber', 'stream']
        self._collections = dict.fromkeys(self._listOfDataClasses)

        self._readStagingFile()

    # _________________________________________________________
    def _readStagingFile(self):
        """Read in staging file."""

        with open( self._stageingFile) as dataFile:    
            setList = json.load(dataFile)
            
            try: 
                self._sets = setList['sets']
            except:
                print('Error reading staging file: no "sets" found')
                sys.exit(-1)

    # _________________________________________________________
    def addCollection(self, dataClass, collection):
        """Get collection from mongoDB."""

        if dataClass not in  self._listOfDataClasses:
            print('Unknown "dataClass"', dataClass, 'for adding collection')
            return False

        self._collections[dataClass] = collection

    # _________________________________________________________
    def _resetAllStagingMarks(self):
        """Reset all staging marks."""

        for dataClass, coll in self._collections.items():
            for target in self._listOfTargets:
                targetField = 'staging.stageMarker{0}'.format(target)
                coll.update_many({}, {'$set': {targetField: False}})

    # _________________________________________________________
    def markFilesToBeStaged(self):
        """Mark files to be staged in staging file."""

        self._resetAllStagingMarks()

        for stageSet in self._sets:
            if not self._prepareSet(stageSet):
                continue
            self._coll.update_many(stageSet, {'$set': {self._targetField: True}})

    # _________________________________________________________
    def listOfFilesToBeStaged(self):
        """Returns a list of all files to be staged"""

        for dataClass, coll in self._collections.items():
            print ('For {0} in collection: {1}'.format(dataClass, coll.name))

            for target in self._listOfTargets:
                targetField = 'staging.stageMarker{0}'.format(target)
                nStaged = coll.find({targetField: True}).count()        
                print('   Files to be staged on {0}: {1}'.format(target, nStaged))

    # _________________________________________________________
    def _prepareSet(self, stageSet):
        """Prepare set to be staged."""

        # -- Check for target 
        try:
            self._target = stageSet['target']
            if self._target not in  self._listOfTargets:
                print('Error reading staging file: Unknown "target"', self._target)
                return False
            self._targetField = "staging.stageMarker{0}".format(self._target)

        except:
            print('Error reading staging file: no "target" found in set' , stageSet)
            self._target = None
            return False

        # -- Check for dataClass 
        try:
            self._dataClass = stageSet['dataClass']
            if self._dataClass not in  self._listOfDataClasses:
                print('Error reading staging file: Unknown "dataClass"', self._dataClass)
                return False
            self._coll = self._collections[self._dataClass]

        except:
            print('Error reading staging file: no "dataClass" found in set' , stageSet)
            self._dataClass = None
            return False

        # -- Clean up 
        del(stageSet['dataClass'])
        del(stageSet['target'])

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

# ____________________________________________________________________________
def main():
    """initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

    stager = stagerSDMS('stagingRequest.json')
    stager.addCollection('picoDst', dbUtil.getCollection("HPSS_PicoDsts"))

    stager.markFilesToBeStaged()
    stager.listOfFilesToBeStaged()

# ------------------------------------------------------------
# WorkFlow to be added
# ------------------------------------------------------------
# - check in xrd_picoList
#   - count staged files per set
#   - if more then 20% missing do staging of all tar files
# ------------------------------------------------------------


    dbUtil.close()
# ____________________________________________________________________________
if __name__ == "__main__":
    print("Start SDMS Stager!")
    sys.exit(main())
