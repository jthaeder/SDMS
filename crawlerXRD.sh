#!/bin/bash -l
#
# run cron scripts
#####################################################
# Script to be called on data server by starxrd user

# ---------------------------------------------------
# XRD:
#  - crawlerXRD.py
#
#####################################################

#####################################################
# -- Source Environemnt
source /global/homes/s/starxrd/bin/.setXRDMongoEnv.sh

#####################################################

module load python/3.4.3

#####################################################

pushd /global/homes/s/starxrd/SDMS > /dev/null

# -- run crawler
python crawlerXRD.py

popd > /dev/null
