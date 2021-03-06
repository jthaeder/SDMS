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
source ~starxrd/bin/.setXRDMongoEnv.sh
source ~starxrd/SDMS/controlSDMS.sh

#####################################################
# -- Load Modules
module load python/3.4.3

#####################################################
# -- Check if script should be run at all
if [ "${runCrawlerXRD}" = "off" ] ; then
  exit 0
fi

#####################################################
# -- Check if script should be run on this node
HOST=`${HOME}/bin/nname 2> /dev/null || uname -n`

echo ${HOST} | egrep -e "${runNotOnNodes}" > /dev/null
if [[ ! -z ${runNotOnNodes} &&  "$?" == "0" ]] ; then
  exit 0
fi

#####################################################

pushd ~starxrd/SDMS > /dev/null

python crawlerXRD.py

# Commented out popd so that this script could be called remotely via xdsh
#popd > /dev/null
