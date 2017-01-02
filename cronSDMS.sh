#!/bin/bash
#
# run cron scripts
#####################################################
# Script to be called by daily cron.
# ---------------------------------------------------
# HPSS:
#  - crawlerHPSS.py
#  - inspectHPSS.py
#
#####################################################

#####################################################
# -- Source Environemnt
source ~starxrd/bin/.setXRDMongoEnv.sh
source ~starxrd/SDMS/controlSDMS.sh

#####################################################
# -- Load Modules
module load python/3.4.3

#####################################################
# -- Check if script should be run
if [ "${runCronSDMS}" == "off" ] ; then
  exit 0
fi

#####################################################
# -- Check if CRON job is still running
for pid in $(pidof -x `basename $0`); do
    if [ $pid != $$ ]; then
        echo "Process is already running with PID $pid"
	exit 0
    fi
done

#####################################################

pushd ~/SDMS > /dev/null

python crawlerHPSS.py

python reportSDMS.py

python inspectHPSS.py

popd > /dev/null
