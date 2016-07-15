#!/bin/bash
#
# Merged out put of splitted checking every file from XRD and DISK
#
# - Argument: inList of files on DISK
#
# Author: Jochen Thaeder                                           

baseDir=/global/homes/j/jthaeder/SDMS/diskToXRootD

############################################################

suffixes=".missing.mendel .ok .wrongSize"

############################################################

if [ $# -ne 1 ] ; then
    echo "No input list as argument"
    exit 1
fi

input=$1 

pushd $baseDir > /dev/null

for suffix in ${suffixes} ; do
    cat results/split/${input}/${input}*${suffix} > results/${input}${suffix}	
done

popd > /dev/null
