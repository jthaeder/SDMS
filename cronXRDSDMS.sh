#!/bin/bash -l
#
# run cron scripts
#####################################################
# Script to be called by daily cron as starxrduser
# ---------------------------------------------------
# XRD:
#  - cleanXRD.py
#  - dataServerCheck.py
#
#####################################################

#####################################################
# -- Source Environemnt
source ~starxrd/bin/.setXRDMongoEnv.sh
source ~starxrd/SDMS/controlSDMS.sh

#####################################################
# -- Load Modules
module load python/3.4.3

module use -a /common/star/pkg/Modules
module load xrootd

#####################################################
# -- Check if script should be run
if [ "${runCronXRDSDMS}" == "off" ] ; then
  exit 0
fi

#####################################################

pushd ~/SDMS > /dev/null

python cleanXRD.py > /dev/null 2>&1

python dataServerCheck.py

popd > /dev/null
