#!/bin/bash
#
# Go over list of picoDst files are check if they are 
# copied correctly
#
# Author: Jochen Thaeder                                           

baseDir=/global/homes/j/jthaeder/SDMS/diskToXRootD

############################################################

pushd $baseDir > /dev/null

input=$1
#input=picoDsts_Run14.in

# -- Prepare of checking
outputOK=results/${input}.ok

checkedOK=${input}.checked.ok
checkedRest=${input}.checked.rest

rm -f $checkedOK $checkedRest
touch $checkedOK $checkedRest

# -- Loop over inList
while read -r line ; do 
    
    grep $line $outputOK > /dev/null
    if [ $? -eq 0 ] ; then
	echo $line >> $checkedOK
    else
	echo $line >> $checkedRest
    fi


done < <( cat $input)


popd > /dev/null