#!/bin/bash -l 
#
# rm files from XRootD
# copied correctly
#
# Author: Jochen Thaeder                                           

baseDir=/global/homes/j/jthaeder/SDMS/diskToXRootD

############################################################

if [ "$CHOS" != "sl62" ] ; then
    echo "Wrong CHOS - should be 'sl62' is '${CHOS}'"
    exit 1
fi

module unload python
module use /common/star/pkg/Modules || exit $?
module load python pymongo star-dm-scripts xrootd || exit $?

pushd $baseDir > /dev/null

input=$1

# -- Loop over inList
while read -r line ; do 
    tmp=`echo $line | awk -F"picodsts" '{ print $2 }'`
    path="/star/picodsts${tmp}"

    echo $path

    xrd pstarxrdr1 rm $path
#    xrd pstarxrdr1 isfileonline $path

done < <( cat $input)


popd > /dev/null