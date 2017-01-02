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

from stagerSDMS import stagerSDMS

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

# ____________________________________________________________________________
def main():
    """Initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

    stager = stagerSDMS(dbUtil, 'stagingRequest.json')

    # -- Clean dummy staged files
    stager.cleanDummyStagedFiles()

    # -- Stage from staging area to staging location
    stager.stageToXRD()

    dbUtil.close()
# ____________________________________________________________________________
if __name__ == "__main__":
    sys.exit(main())
