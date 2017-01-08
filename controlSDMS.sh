#!/bin/bash
#
# Script to control different cron jobs
#####################################################
# Script is sourced by
#  - cronSDMS.sh
#  - cronSDMSHourly.sh
#  - cronXRDSDMS.sh
#  - crawlerXRD.sh
#  - stagerSDMSXRD.sh
#  - stagerSDMSHPSS.sh
#####################################################

# ---------------------------------------------------
# -- MASTER SWITCHES
# ---------------------------------------------------
# - Run no SDMS at all -> Set to off
runSDMS=on
# ---------------------------------------------------
# - No MongoDB availble -> Set to off
runOnMongoDB=on
# ---------------------------------------------------
# - No XRD availble -> Set to off
runOnXRD=on
# ---------------------------------------------------
# - No HPSS availble -> Set to off
runOnHPSS=on
# ---------------------------------------------------

# ---------------------------------------------------
# -- Runs Daily CRON : cronSDMS.sh
# ---------------------------------------------------
#    Runs on HPSS:
#     - crawlerHPSS.py
#     - inspectHPSS.py
# ---------------------------------------------------
#    Current CRON options:
#      node: pdsf8
#      user: starofl
#      schedule: 37 16 * * *
# ---------------------------------------------------
#   Use "on" or "off"
runCronSDMS=on

# ---------------------------------------------------
# -- Runs Hourly CRON : cronSDMSHourly.sh
# ---------------------------------------------------
#    Runs XRD:
#     - processXRD.py
#    Runs Stager (HPSS and XRD)
#     - stagerSDMS.py
# ---------------------------------------------------
#    Current CRON options:
#      node: pdsf8
#      user: starofl
#      schedule: 07 * * * *
# ---------------------------------------------------
#   Use "on" or "off"
runCronSDMSHourly=on

# ---------------------------------------------------
# -- Runs Hourly CRON : stagerSDMSXRD.sh
#                       stagerSDMSHPSS.sh
# ---------------------------------------------------
#    Runs Stager (HPSS and XRD)
#     - stagerSDMSXRD.sh
#     - stagerSDMSHPSS.sh
# ---------------------------------------------------
#    Current CRON options:
#      node: pdsf8
#      user: starofl
#      schedule: 17 * * * *
# ---------------------------------------------------
#   Use "on" or "off"
runCronStaging=on

# ---------------------------------------------------
# -- Runs Hourly CRON : runCronXRDSDMS.sh
# ---------------------------------------------------
#    Runs XRD:
#     - cleanXRD.py
#     - dataServerCheck.py
# ---------------------------------------------------
#    Current CRON options:
#      node: pstarxrdr1
#      user: starxrd
#      schedule: 51 * * * *
# ---------------------------------------------------
#   Use "on" or "off"
runCronXRDSDMS=on

# ---------------------------------------------------
# -- Runs Daily CRON : runCrawlerXRD.sh
# ---------------------------------------------------
#    Runs XRD:
#     - crawlerXRD.py
# ---------------------------------------------------
#    Current CRON options:
#      node: <nodes>
#      user: starxrd
#      # schedule: 51 * * * *
# ---------------------------------------------------
#   Use "on" or "off"
runCrawlerXRD=on
#   Regex of nodes not to run, eg.
#   - runNotOnNodes="mc0301|mc0201"
#   - runNotOnNodes="mc020[0-1]"
#   - runNotOnNodes="" < run on all nodes
runNotOnNodes=""

# ---------------------------------------------------
# -- EXECUTE MASTER SWITCHES - DO NOT CHANGE
# ---------------------------------------------------
if [[ "${runSDMS}" == "off" || "${runOnMongoDB}" == "off" ]] ; then
  runCronSDMS=off
  runCronSDMSHourly=off
  runCronXRDSDMS=off
  runCrawlerXRD=off
  runCronStaging=off
fi

if [ "${runOnXRD}" == "off" ] ; then
  runCronXRDSDMS=off
  runCronSDMSHourly=off
  runCronStaging=off
fi

if [ "${runOnHPSS}" == "off" ] ; then
  runCronSDMS=off
  runCronSDMSHourly=off
  runCronStaging=off
fi
