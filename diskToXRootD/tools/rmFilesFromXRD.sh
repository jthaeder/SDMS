#!/bin/bash -l 
#
# rm files from XRootD using an in List as argument
#
# - Argument: inList of files on XRD Disk
#     -> Dump of "fileFullPath" column from MongoDB
#
#   Valid file layouts:
#
#      fileFullPath
#      "/export/data/xrd/ns/star/reco/ppProductionMB62/....1060005.MuDst.root"
#      "/export/data/xrd/ns/star/reco/ppProductionMB62/....1020001.MuDst.root"
#      ....
#
#      /export/data/xrd/ns/star/reco/ppProductionMB62/....1060005.MuDst.root
#      /export/data/xrd/ns/star/reco/ppProductionMB62/....1020001.MuDst.root
#      ....
#
# - Run in CHOS sl62
#    $  basedir=/global/homes/j/jthaeder/SDMS/diskToXRootD
#    $  ${basedir}/runInCHOS.sh ${basedir}/tools/rmFilesFromXRD.sh 
#
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

    echo $line | grep "^\"" > /dev/null
    if [ $? -eq 0 ] ; then 
	path=`echo $line | awk -F'"' '{ print $2 }'`
    else
	path=$line
    fi
    
    if [ "$path" == "fileFullPath" ] ; then 
	continue
    fi

    echo $path | grep "^/export/data/xrd/ns/" > /dev/null
    if [ $? -eq 0 ] ; then 
	pathX=`echo $path | awk -F'/export/data/xrd/ns/' '{ print $2 }'`
	path=$pathX
    fi

    echo $path

    xrd pstarxrdr1 rm $path
#    xrd pstarxrdr1 isfileonline $path

done < <( cat $input)


popd > /dev/null