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

echo "START SDMS daily cron - as starxrd!"
echo "-----------------------------------"
echo " "
date
echo " "

#####################################################
# -- Source Environemnt
source /global/homes/s/starxrd/bin/.setXRDMongoEnv.sh

#####################################################
# -- Check if CRON job is still running
for pid in $(pidof -x `basename $0`); do
    if [ $pid != $$ ]; then
        echo "Process is already running with PID $pid"
	exit 0
    fi
done

#####################################################

module load python/3.4.3

module use -a /common/star/pkg/Modules
module load xrootd

#####################################################

pushd /global/homes/s/starxrd/SDMS > /dev/null

# -- clean XRD
python cleanXRD.py

# -- check data server
python dataServerCheck.py

popd > /dev/null
