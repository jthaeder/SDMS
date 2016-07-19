#!/bin/bash

# run cron scripts

echo "START SDMS daily cron!"
echo "-----------------------------------"
echo " "
date
echo " "

###############################

# -- Source Environemnt
source /global/homes/j/jthaeder/bin/setbash.sh


pushd /global/homes/j/jthaeder/SDMS > /dev/null

echo " "
echo "-----------------------------------"
echo "START SDMS - HPSS Crawler"
echo "-----------------------------------"
echo " "

python crawlerHPSS.py

echo " "
echo "-----------------------------------"
echo "STOP SDMS - HPSS Crawler"
echo "-----------------------------------"
echo " "
echo "-----------------------------------"
echo "START SDMS - HPSS inspector"
echo "-----------------------------------"
echo " "

python inspectHPSS.py

echo " "
echo "-----------------------------------"
echo "STOPT SDMS - HPSS inspector"
echo "-----------------------------------"
echo " "

popd > /dev/null