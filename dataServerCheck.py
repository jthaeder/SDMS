#!/usr/bin/env python
b'This script requires python 3.4'

"""

"""

import sys
import os
import os.path
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

DATA_SERVERS = "${ALL_DATASERVERS}"
SOCKET_TIMEOUT = 5

# ${PDSF_DATASERVERS}
# ${MENDEL_DATASERVERS}

##############################################

# -- Check for a proper Python Version
if sys.version[0:3] < '3.0':
    print ('Python version 3.0 or greater required (found: {0}).'.format(sys.version[0:5]))
    sys.exit(-1)


# ----------------------------------------------------------------------------------
class dataServerCheck:
    """Check all XRD dataServers"""

    # _________________________________________________________
    def __init__(self, clusterEnvFile):
        self._today = datetime.datetime.today().strftime('%Y-%m-%d')
        self._clusterEnvFile = clusterEnvFile
        self._listOfTargets = ['dataServerXRD']
        self._collections = dict.fromkeys(self._listOfTargets)

        self._target = ''

        self._listOfDataServersXRD = []

        self._processClusterEnvFile()

    # _________________________________________________________
    def _processClusterEnvFile(self):
        """Read env file and get list of active nodes"""

        if not os.path.isfile(self._clusterEnvFile):
            print ('Cluster env file {0} does not exist!'.format(self._clusterEnvFile))
            sys.exit(-1)

        # -- get data servers from cluster.env file
        cmdLine = "bash -c 'source {0} && echo {1}'".format(self._clusterEnvFile, DATA_SERVERS)
        cmd = shlex.split(cmdLine)

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        for serverListString in iter(p.stdout.readline, b''):
            self._listOfDataServersXRD = serverListString.decode("utf-8").rstrip().split()

    # _________________________________________________________
    def addCollection(self, target, collection):
        """Get collection from mongoDB."""

        if target not in  self._listOfTargets:
            print('Unknown "target"', target, 'for adding collection')
            return False

        self._collections[target] = collection

    # _________________________________________________________
    def createReport(self, target):
        """creatw a report on the list of servers of the target"""

        if target not in  self._listOfTargets:
            print('Unknown "target"', target, 'for reporting')
            return False

        self._target = target

        # -- Get list of already active or inactive server
        self._listOfActiveServers = [d['nodeName'] for d in self._collections[self._target].find({'stateActive': True},
                                                                                                 {'nodeName': True, '_id': False})]

        self._listOfInactiveServers = [d['nodeName'] for d in self._collections[self._target].find({'stateActive': False},
                                                                                                   {'nodeName': True, '_id': False})]

        # -- Prepare list of changes
        self._listOfNowInactiveServers = []
        self._listOfNowActiveServers = []
        self._listOfNewServers = []

        # -- Update DB with actual state
        self._updateDataServerList()

        # -- Report changes
        if len(self._listOfNowInactiveServers) or len(self._listOfNowActiveServers) or len(self._listOfNewServers):
            print("--------------------------------------------")
            if len(self._listOfNowInactiveServers):
                print("Now inactive: ", self._listOfNowInactiveServers)
            if len(self._listOfNowActiveServers):
                print("Now active:   ", self._listOfNowActiveServers)
            if len(self._listOfNewServers):
                print("--------------------------------------------")
                print("New Servers:  ", self._listOfNewServers)
            print("--------------------------------------------")

        # -- Report inactive
        print("List of Inactive Nodes:")
        for entry in self._collections[self._target].find({'stateActive': False}, {'nodeName': True, '_id': False, 'setInactive':True}):
            print(entry)



    # _________________________________________________________
    def _updateDataServerList(self):
        """update list of servers"""

        for server in self._listOfDataServersXRD:
            isActive = self._checkServer(server)

    # _________________________________________________________
    def _checkServer(self, server):
        """check server

            return true for active server
            return false for inactive server
            """

        # -- Check is server is active
        isServerActive = self._isServerActive(server)

        # -- Create node document
        doc = {
               'nodeName': server,
               'lastWalkerRun' : '2000-01-01',
               'totalSpace': -1,
               'usedSpace': -1,
               'freeSpace': -1
               }

        # -- Check for state changes
        #    Update the DB and lastSeen

        # --- was active before
        if server in self._listOfActiveServers:
            self._collections[self._target].find_one_and_update({'nodeName': doc['nodeName']}, {'$set': {'lastSeen': self._today}})

            # ---- now inactive
            if not isServerActive:
                self._listOfNowInactiveServers.append(server)
                self._collections[self._target].find_one_and_update({'nodeName': doc['nodeName']},
                                                                    {'$set': {'setInactive': self._today, 'stateActive': isServerActive}})

        # --- was inactive before
        elif server in self._listOfInactiveServers:
            self._collections[self._target].find_one_and_update({'nodeName': doc['nodeName']}, {'$set': {'lastSeen': self._today}})

            # ---- now active
            if isServerActive:
                self._listOfNowActiveServers.append(server)
                self._collections[self._target].find_one_and_update({'nodeName': doc['nodeName']},
                                                                    {'$set': {'setInactive': -1, 'stateActive': isServerActive}})
        # --- new
        else:
            self._listOfNewServers.append(server)

            # ---- now active
            if isServerActive:
                self._listOfNowActiveServers.append(server)
                self._collections[self._target].find_one_and_update({'nodeName': doc['nodeName']},
                                                                    {'$set': {'lastSeen': self._today,
                                                                              'setInactive': -1,
                                                                              'stateActive': isServerActive},
                                                                     '$setOnInsert' : doc}, upsert = True)
            # ---- now inactive
            else:
                self._listOfNowInactiveServers.append(server)
                self._collections[self._target].find_one_and_update({'nodeName': doc['nodeName']},
                                                                    {'$set': {'lastSeen': -1,
                                                                              'setInactive': self._today,
                                                                              'stateActive': isServerActive},
                                                                     '$setOnInsert' : doc}, upsert = True)


    # _________________________________________________________
    def _isServerActive(self, server):
        """check server

            return true for active server
            return false for inactive server
            """

        isServerActive = True;
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(SOCKET_TIMEOUT)

        try:
            sock.connect((server, 22))
        except socket.error:
            isServerActive = False
            sock.close()

        sock.close()

        return isServerActive

# ____________________________________________________________________________
def main():
    """initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

    serverCheck = dataServerCheck('/global/homes/s/starxrd/bin/cluster.env')

    serverCheck.addCollection('dataServerXRD', dbUtil.getCollection("XRD_DataServers"))

    serverCheck.createReport('dataServerXRD')

    dbUtil.close()

# ____________________________________________________________________________
if __name__ == "__main__":
    print("Start XRD dataServer check")
    sys.exit(main())
