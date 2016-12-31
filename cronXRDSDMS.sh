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
source ~/bin/.setXRDMongoEnv.sh

#####################################################

module load python/3.4.3

module use -a /common/star/pkg/Modules
module load xrootd

#####################################################

pushd ~/SDMS > /dev/null

# -- clean XRD
python cleanXRD.py > /dev/null 2>&1

# -- check data server
python dataServerCheck.py

popd > /dev/null
