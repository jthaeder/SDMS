#!/usr/bin/env python
b'This script requires python 3.4'

"""
Stager which reads staging file and sets stageMarker in HPSSPicoDsts collection

The staging file can have several sets, 

each set can have the following parameters

 'target'   : 'XRD'           (For now the only option)
 'dataClass': 'picoDst'       (For now the only option)

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
        self._listOfTargets = {'XRD'}
        self._listOfDataClasses = {'picoDst'}
        self._listOfQueryItem = {'runyear', 'system', 'energy', 'trigger', 'production', 'day', 'runnumber', 'stream'}

        self._readStagingFile()

    # _________________________________________________________
    def setCollections(self, collHpssPicoDsts):
        """Get collection from mongoDB."""
        
        self._collHpssPicoDsts = collHpssPicoDsts

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
    def _resetAllStagingMarks(self):
        """Reset all staging marks.
        
           Add a line for every collection / target 
           """
        print("reset all")
        #self._collHpssPicoDsts.UpdateMany({}, {'$set': {'staging.stageMarkerXRD': False}})


    # _________________________________________________________
    def markFilesToBeStaged(self):
        """Mark files to be staged in staging file."""

        self._resetAllStagingMarks()

        for stageSet in self._sets:
            if not self._prepareSet(stageSet):
                continue

            print( self._targetField )

           # self._collHpssPicoDsts.UpdateMany(stageSet, {'$set': {'staging.stageMarkerXRD': True}})

    # _________________________________________________________
    def listOfFilesToBeStaged(self):
        """Returns a list of all files to be staged"""
        
        nStaged = self._collHpssPicoDsts.Find({'staging.stageMarkerXRD': True}).count()
        print("Number of files to be staged:", nStaged)

    # _________________________________________________________
    def _prepareSet(self, stageSet):
        """Prepare set to be staged."""

        # -- Check for target 
        try:
            self._target = stageSet['target']
            if self._target not in  self._listOfTargets:
                print('Error reading staging file: Unknown "target"', self._target)
                return False
            if self._target == 'picoDsts':
                self._coll = self._collHpssPicoDsts 

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
            self._targetField = "staging.stageMarker{0}".format(self._dataClass)

        except:
            print('Error reading staging file: no "dataClass" found in set' , stageSet)
            self._dataClass = None
            return False

        # -- Clean up 
        del(stageSet['dataClass'])
        del(stageSet['target'])

        # -- Check if query items are correct
        for key, value in stageSet.items():
            if key not in self._listOfQueryItem:
                print('Error reading staging file: Query item does not exsist:', key)
                return False

        return True



# ____________________________________________________________________________
def main():
    """initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")
    collHpssPicoDsts = dbUtil.getCollection("HPSS_PicoDsts")

    stager = stagerSDMS('stagingRequest.json')
    stager.setCollections(collHpssPicoDsts)
    stager.markFilesToBeStaged()
    

    dbUtil.close()
# ____________________________________________________________________________
if __name__ == "__main__":
    print("Start SDMS Stager!")
    sys.exit(main())
