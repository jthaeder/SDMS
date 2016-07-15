#!/bin/bash
#
# Split input list and check every file from XRD and DISK
# with submitted jobs 
#
# - Argument: inList of files on DISK
#
# Author: Jochen Thaeder                                           

baseDir=/global/homes/j/jthaeder/SDMS/diskToXRootD

############################################################

splitCount=2000

############################################################

if [ $# -ne 1 ] ; then
    echo "No input list as argument"
    exit 1
fi

input=$1 

pushd $baseDir > /dev/null

mkdir -p split/$input/
mkdir -p logs/split/$input/

split -l ${splitCount} ${input} split/${input}/${input}.

pushd split/$input/ > /dev/null

for file in `ls` ; do
    qsub -j y -o ${baseDir}/logs/split/${input} ${baseDir}/checkIfFilesAreInXRootD.sh split/${input}/$file
done

popd > /dev/null
popd > /dev/null