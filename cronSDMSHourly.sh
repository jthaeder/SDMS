#!/bin/bash -l
#
# run cron scripts
#####################################################
# Script to be called by hourly cron.
# ---------------------------------------------------
# XRD:
#  - processXRD.py
# Stager
#  - stagerSDMS.py
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
if [ "${runCronSDMSHourly}" == "off" ] ; then
  exit 0
fi

#####################################################

pushd ~/SDMS > /dev/null

python processXRD.py

python stagerSDMS.py

popd > /dev/null
