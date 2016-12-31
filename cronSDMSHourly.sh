#!/bin/bash -l
#
# run cron scripts
#####################################################
# Script to be called by daily cron.
# Runs different suites for SDMS
# ---------------------------------------------------
# HPSS:
#  - crawlerHPSS.py
#  - inspectHPSS.py
#
#####################################################


#####################################################
# -- Source Environemnt
source /global/homes/j/jthaeder/bin/setbash.sh
#source /global/homes/s/starxrd/bin/.setXRDMongoEnv.sh

#####################################################

module load python/3.4.3

module use -a /common/star/pkg/Modules
module load xrootd

#####################################################

pushd /global/homes/j/jthaeder/SDMS > /dev/null

# -- processXRD.py
python processXRD.py

# -- stage
python stagerSDMS.py

popd > /dev/null
