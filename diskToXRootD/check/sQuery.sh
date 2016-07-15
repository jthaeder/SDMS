#!/bin/bash -l

xpath=$1

module unload python
module use /common/star/pkg/Modules || exit $?
module load python pymongo star-dm-scripts || exit $?

nCounts=`/common/star/pkg/scripts/starquery --mongo-db-coll picodsts '{ "fullpath": "'${xpath}'" }' | wc -l`
tmpsize=`/common/star/pkg/scripts/starquery --mongo-db-coll picodsts '{ "fullpath": "'${xpath}'" }' --raw | grep size | sort | uniq | head -n 1`
storageDetail=`/common/star/pkg/scripts/starquery --mongo-db-coll picodsts '{ "fullpath": "'${xpath}'" }' --raw | grep storage_detail | sort | uniq`
size=`echo $tmpsize | awk -F':' '{ print $2 }' | awk -F',' '{ print $1 }' `

echo $storageDetail | grep mc > /dev/null
isOnMendel=$?
if [ ! $isOnMendel ] ; then
    isOnMendel="isOnMendel"
fi

echo $storageDetail | grep pc > /dev/null
isOnPDSF=$?
if [ ! $isOnPDSF ] ; then
    isOnPDSF="isOnPDSF"
fi

echo "${nCounts}:${size}:${isOnMendel}:${isOnPDSF}"
