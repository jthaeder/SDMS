#!/bin/bash
#
# Check if all tar folders have been created 
# 
# Author: Jochen Thaeder

basePath=/global/homes/j/jthaeder/SDMS/tarToHPSS
projectBaseDir=/project/projectdirs/starprod/picodsts
hpssBaseDir=/nersc/projects/starofl/picodsts

############################################################

# -- run year
runs="Run14"
                                                                                                                                    
# -- subset of production
production="AuAu/200GeV/physics2/P15ic"

############################################################

pushd ${projectBaseDir} > /dev/null

for run in $runs ; do 
    echo "Process ${run}"

    # -- get list of files from HPSS
    hsi -P find ${hpssBaseDir}/${run}/${production} -name "*.tar" > ${basePath}/${run}_picoList.lst.hpss
    hsi -P find ${hpssBaseDir}/${run}/${production} -name "*.tar.idx" > ${basePath}/${run}_picoList.idx.lst.hpss

    while read -r line ; do                                                                                       
        day=`basename ${line}`                                                                                    
        folder=`dirname ${line}`                                                                                  

	echo $folder | grep fileLists > /dev/null
	if [ $? -eq 0 ] ; then
	    continue
	fi

	# -- pick proper production 
	echo $folder | grep -v ${production} > /dev/null
	if [ $? -eq 0 ] ; then
	    continue
	fi

	# -- build in and out paths
	tarFile=${day}.tar
	idxFile=${day}.tar.idx
	tarFolder=${hpssBaseDir}/${folder}
	projectFolder=${projectBaseDir}/${line}
	
	grep "${tarFolder}/${tarFile}" ${basePath}/${run}_picoList.lst.hpss > /dev/null
        if [ $? -ne 0 ] ; then
            echo "${tarFolder}/${tarFile} missing"
        fi

	grep "${tarFolder}/${idxFile}" ${basePath}/${run}_picoList.idx.lst.hpss > /dev/null
        if [ $? -ne 0 ] ; then
            echo "${tarFolder}/${idxFile} missing"
        fi
        
    done < <(find ${run} -mindepth 5 -maxdepth 5 -type d)       
done 

popd > /dev/null
