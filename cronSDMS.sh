#!/bin/bash
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

echo "START SDMS daily cron!"
echo "-----------------------------------"
echo " "
date
echo " "

#####################################################
# -- Source Environemnt
source /global/homes/j/jthaeder/bin/setbash.sh

#####################################################
# -- Check if CRON job is still running
for pid in $(pidof -x `basename $0`); do
    if [ $pid != $$ ]; then
        echo "Process is already running with PID $pid"
	exit 0
    fi
done

#####################################################

pushd /global/homes/j/jthaeder/SDMS > /dev/null

python crawlerHPSS.py

python inspectHPSS.py

popd > /dev/null
