#!/bin/bash -l 
#
# cp files from XRootD to local disk
#
#  ./cpFilesFromXRD.sh <inList> <targetDirectory>
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
#    $  ${basedir}/runInCHOS.sh ${basedir}/tools/cpFilesFromXRD.sh <inList> <targetDirectory>
#
#
# Author: Jochen Thaeder                                           

baseDir=/global/homes/j/jthaeder/SDMS/diskToXRootD

############################################################

if [ "$CHOS" != "sl62" ] ; then
    echo "Wrong CHOS - should be 'sl62' is '${CHOS}'"
    exit 1
fi


input=$1
target=$2

if [ $# -ne 2 ] ; then
    echo "Usage $0 <fileName> <targetPath>"
    exit 1
fi
    
module unload python
module use /common/star/pkg/Modules || exit $?
module load python pymongo star-dm-scripts xrootd || exit $?

XRD_CMD="xrdcp"
XRD_PREFIX="xroot://pstarxrdr1.nersc.gov:1094/"

pushd $baseDir > /dev/null

# -- Loop over inList
while read -r line ; do 

    if [ "$line" == "fileFullPath" ] ; then
	continue
    fi

    echo $line | grep "^\"" > /dev/null
    if [ $? -eq 0 ] ; then 
	path=`echo $line | awk -F'"' '{ print $2 }'`
    else
	path=$line
    fi
    
    echo $path | grep "^/export/data/xrd/ns/" > /dev/null
    if [ $? -eq 0 ] ; then 
	pathX=`echo $path | awk -F'/export/data/xrd/ns/' '{ print $2 }'`
	path=$pathX
    fi

    targetPath=${target}/`dirname $path`
    targetFile=${target}/$path

    mkdir -p $targetPath
    ${XRD_CMD} ${XRD_PREFIX}/${path} ${targetFile}
done < <( cat $input)


popd > /dev/null