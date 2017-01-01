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
#source ~starxrd/bin/.setXRDMongoEnv.sh
#source ~starxrd/SDMS/controlSDMS.sh
source ./controlSDMS.sh

#####################################################
# -- Load Modules
#module load python/3.4.3

#####################################################
# -- Check if script should be run at all
if [ "${runCrawlerXRD}" = "off" ] ; then
  exit 0
fi

#####################################################
# -- Check if script should be run on this node

HOST=mc0201
echo ${runNotOnNodes}
echo ${HOST} | egrep -e "${runNotOnNodes}" > /dev/null
if [[ ! -z ${runNotOnNodes} &&  "$?" == "0" ]] ; then
  echo "don;t run"
  exit 0
fi


echo "run"

exit


#####################################################

pushd ~starxrd/SDMS > /dev/null

python crawlerXRD.py

popd > /dev/null
