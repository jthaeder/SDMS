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

pushd /global/homes/j/jthaeder/SDMS > /dev/null

echo "START SDMS - XRD processing"

# -- processXRD.py
#python processXRD.py

# -- stage 
python stagerSDMS.py

popd > /dev/null
