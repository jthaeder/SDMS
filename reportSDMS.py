#!/usr/bin/env python
b'This script requires python 3.4'

"""
Script which reports about the status of SDMS.
To be run in daily cron job

For detailed documentation, see: README_XRD.md#xrd-check
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
from dataServerCheck import dataServerCheck
import pymongo

from pymongo import results
from pymongo import errors
from pymongo import bulk

from pprint import pprint

from stagerSDMS import stagerSDMS

# -- Check for a proper Python Version
if sys.version[0:3] < '3.0':
    print ('Python version 3.0 or greater required (found: {0}).'.format(sys.version[0:5]))
    sys.exit(-1)

# ____________________________________________________________________________
def main():
    """initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

    serverCheck = dataServerCheck('/global/homes/s/starxrd/bin/cluster.env', dbUtil)
    # -- Create report of active and inactive servers
    serverCheck.createFullReport()

    stager = stagerSDMS(dbUtil, 'stagingRequest.json')
    stager.cleanDummyStagedFiles()  
    # -- Print staging stats
    stager.printStagingStats()

    dbUtil.close()

# ____________________________________________________________________________
if __name__ == "__main__":
    sys.exit(main())
