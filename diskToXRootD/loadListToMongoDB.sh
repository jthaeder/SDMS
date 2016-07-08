#!/bin/bash 

#
# preload list into MongoDB
#  - Needes List as argument
#
# Author: Jochen Thaeder  

baseDir=/global/homes/j/jthaeder/SDMS/diskToXRootD

############################################################

if [ $# -ne 1 ] ; then 
    echo "Missing argument: ListName"
    exit 1
fi

ListName=$1

if [ "$CHOS" != "sl62" ] ; then
    echo "Wrong CHOS - should be 'sl62' is '${CHOS}'"
    exit 1
fi

module unload python
module use /common/star/pkg/Modules
module load python pymongo star-dm-scripts xrootd


echo "Preload List:m ${ListName}"

preloader $ListName

echo "... done"




