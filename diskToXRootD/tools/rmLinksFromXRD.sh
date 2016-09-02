#!/bin/bash -l 
#
# rm links from XRootD which don't have data behind
#
#  ./rmLinksFromXRD.sh <inList>
#
# - Argument: inList of files on XRD Disk
#     -> Dump of "fileFullPath" and "storage.detail" columns from MongoDB
#
#   Valid file layouts:
#
#      fileFullPath,storage.detail
#      "/export/data/xrd/ns/star/reco/ppProductionMB62/....1060005.MuDst.root",mc0115
#      "/export/data/xrd/ns/star/reco/ppProductionMB62/....1020001.MuDst.root",mc0115
#      ....
#
#      /export/data/xrd/ns/star/reco/ppProductionMB62/....1060005.MuDst.root,mc0115
#      /export/data/xrd/ns/star/reco/ppProductionMB62/....1020001.MuDst.root,mc0115
#      ....
#
# - Run in CHOS sl62
#    $  basedir=/global/homes/j/jthaeder/SDMS/diskToXRootD
#    $  ${basedir}/runInCHOS.sh ${basedir}/tools/rmLinksFromXRD.sh <inList>
#
#     
# Author: Jochen Thaeder    


# Input file is expected to be like :
#
# fileFullPath,storage.detail
#"/export/data/xrd/ns/star/reco/ppProduction62/FullField/P12ia/2006/160/7160006/st_physics_7160006_raw_1040017.MuDst.root",pc1826
#"/export/data/xrd/ns/star/reco/ppProductionMB62/FullField/P12ia/2006/171/7171001/st_physics_7171001_raw_1020006.MuDst.root",pc1826
#
# ...
#
#
# run this script as starxrd user
#
# Author: Jochen Thaeder                                           

baseDir=/global/homes/j/jthaeder/SDMS/diskToXRootD

############################################################

pushd $baseDir > /dev/null

input=$1



# -- Loop over inList
while read -r line ; do 

    path=`echo $line | awk -F',' '{ print $1 }'`
    node=`echo $line | awk -F',' '{ print $2 }'`

    if [ "$path" == "fileFullPath" ] ; then
	continue
    fi
    
    echo $path | grep "^\"" > /dev/null
    if [ $? -eq 0 ] ; then 
	pathX=`echo $line | awk -F'"' '{ print $2 }'`
	path=$pathX
    fi
    
    echo $path $node
    ssh -q $node "rm -f $path" < /dev/null

done < <( cat $input)


popd > /dev/null