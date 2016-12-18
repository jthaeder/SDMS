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

SOCKET_TIMEOUT = 5

ENV_FIELDS = {'META_MANAGER': "${META_MANAGER}",
              'MENDEL_ONE_MANAGER': "${MENDEL_ONE_MANAGER}",
              'MENDEL_TWO_MANAGER': "${MENDEL_TWO_MANAGER}",
              'MENDEL_ONE_SUPERVISOR': "${MENDEL_ONE_SUPERVISORS}",
              'MENDEL_TWO_SUPERVISOR': "${MENDEL_TWO_SUPERVISORS}",
              'DATASERVER': "${ALL_DATASERVERS}",
              'MENDEL_ONE_DATASERVER': "${MENDEL_ONE_DATASERVERS}",
              'MENDEL_TWO_DATASERVER': "${MENDEL_TWO_DATASERVERS}"}

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

        #self._listOfTargets = ['dataServerXRD']
        #self._collections = dict.fromkeys(self._listOfTargets)

        self._processClusterEnvFile()

    # _________________________________________________________
    def _processClusterEnvFile(self):
        """Read env file and get list of active nodes"""

        if not os.path.isfile(self._clusterEnvFile):
            print ('Cluster env file {0} does not exist!'.format(self._clusterEnvFile))
            sys.exit(-1)

        self._nodeRoleList = {}
        for key, value in ENV_FIELDS.items():
            self._nodeRoleList[key] = self._processClusterEnvField(value)

    # _________________________________________________________
    def _processClusterEnvField(self, envField):
        """Process one variable in cluster.env and return list."""

        cmdLine = "bash -c 'source {0} && echo {1}'".format(self._clusterEnvFile, envField)
        cmd = shlex.split(cmdLine)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        for serverListString in iter(p.stdout.readline, b''):
            envList = serverListString.decode("utf-8").rstrip().split()

        return envList

    # _________________________________________________________
    def addCollection(self, collection):
        """Get collection from mongoDB."""

        self._collServerXRD = collection

    # _________________________________________________________
    def prepareReport(self):
        """Get status before the update."""

        # -- Get list of already active or inactive server
        self._listOfActiveServers = set(d['nodeName'] for d in self._collServerXRD.find({'stateActive': True}))
        self._listOfInactiveServers = set(d['nodeName'] for d in self._collServerXRD.find({'stateActive': False}))

        # -- Get list of all data servers
        self._listOfDataServers = set(d['nodeName'] for d in self._collServerXRD.find({'role': 'DATASERVER'}))

        # -- Prepare list of changes
        self._listOfNowInactiveServers = []
        self._listOfNowActiveServers = []

    # _________________________________________________________
    def updateAllServerList(self):
        """Update all servers state active/inactive"""

        # -- Create superset of all nodes which are and could be there
        allServerSet = set(self._nodeRoleList['ALL_DATASERVER'])
        allServerSet.update(self._listOfActiveServers)
        allServerSet.update(self._listOfInactiveServers)
        allServerSet.update(self._listOfDataServers)

        # -- Check if server node is active and update
        #    add new node
        for server in allServerSet:
            self._processServer(server)

    # _________________________________________________________
    def _processServer(self, server):
        """Process one server according to it being active."""

        # -- Check is server is active
        isServerActive = self._isServerActive(server)

        # -- Create server document (is it does not exist yet)
        doc = {
               'nodeName': server,
               'lastCrawlerRun': '2000-01-01',
               'totalSpace': -1,
               'usedSpace': -1,
               'freeSpace': -1
               }

        # -- Check for state changes and update fields
        if isServerActive:
            self._listOfNowActiveServers.append(server)
            self._collections[self._target].find_one_and_update({'nodeName': doc['nodeName']},
                                                                {'$set': {'lastSeenActive': self._today,
                                                                           'stateActive': isServerActive},
                                                                 '$setOnInsert' : doc}, upsert = True)

        else:
            self._listOfNowInactiveServers.append(server)
            doc['lastSeenActive'] = "2000-01-01"
            self._collections[self._target].find_one_and_update({'nodeName': doc['nodeName']},
                                                                {'$set': {'stateActive': isServerActive},
                                                                 '$setOnInsert' : doc}, upsert = True)

    # _________________________________________________________
    def _isServerActive(self, server):
        """Check if server is active or inactive."""

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

    # _________________________________________________________
    def setServerRoles(self):
        """Set roles of server according to cluster.env file."""

        # -- Loop over all nodes and get their role
        for docNode in self._collServerXRD.find({}):

            roleSet = set()
            for key, value in ENV_FIELDS.items():
                if docNode['nodeName'] in self._nodeRoleList[key]:
                    roleSet.add(key)

            # -- if node has at least one role, set as inClusterXRD
            if len(roleSet) > 0:
                inClusterXRD = True
            else:
                inClusterXRD = False

            self._collServerXRD.find_one_and_update({'_id': docNode['_id']},
                                                    {'$set': {'roles': roleSet,
                                                              'inClusterXRD': inClusterXRD}})

    # _________________________________________________________
    def createReport(self):
        """Create a report on the list of servers of the target."""

        # -- Get new list of all data servers
        self._listOfNowDataServers = set(d['nodeName'] for d in self._collections[self._target].find({'role': 'DATASERVER'}))

        # -- New active server
        listOfNewActiveServer = self._listOfNowActiveServers.difference(self._listOfActiveServers)
        if (len(listOfNewActiveServer)):
            print("Now active: ", listOfNewActiveServer)

        # -- New inactive server
        listOfNewInactiveServer = self._listOfNowInactiveServers.difference(self._listOfInactiveServers)
        if (len(listOfNewInactiveServer)):
            print("Now inactive: ", listOfNewInactiveServer)

        # -- New XRD data server
        listOfNewDataServers = self.listOfNowDataServers.difference(self.listOfDataServers)
        if (len(listOfNewDataServers)):
            print("Now data server: ", listOfNewDataServers)

        # -- New Missing XRD data server
        listOfNewMissingDataServers = self.listOfDataServers.difference(self.listOfNowDataServers)
        if (len(listOfNewMissingDataServers)):
            print("Now missing data server: ", listOfNewMissingDataServers)

        # -- Inactive XRD data server
        inactiveServerXRD = set(d['nodeName'] for d in self._collServerXRD.find({'role': 'DATASERVER', 'stateActive': False}))
        if (len(inactiveServerXRD)):
            print("Inactive data server: ", inactiveServerXRD)

        # -- Inactive server with data on them


# ____________________________________________________________________________
def main():
    """initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

    serverCheck = dataServerCheck('/global/homes/s/starxrd/bin/cluster.env')
    serverCheck.addCollection(dbUtil.getCollection("XRD_DataServers"))

    serverCheck.prepareReport()

    # -- Set server roles
    serverCheck.setServerRoles()

    # -- Create report of active and inactive servers
    serverCheck.updateAllServerList()
    serverCheck.setServerRoles()

    # -- Create report of active and inactive servers
    serverCheck.createReport()

    dbUtil.close()

# ____________________________________________________________________________
if __name__ == "__main__":
    print("Start XRD dataServer check")
    sys.exit(main())
