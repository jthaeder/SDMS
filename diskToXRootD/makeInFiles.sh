#!/bin/bash
#
# Build lists of picoDsts - to be copied to HPSS
#
# Author: Jochen Thaeder                                           

baseDir=/global/homes/j/jthaeder/SDMS/diskToXRootD
projectBaseDir=/project/projectdirs/starprod/picodsts

############################################################

# -- run year
runs="Run14"

# -- subset of production
production="200GeV/physics2/P15ic"

############################################################

pushd ${projectBaseDir} > /dev/null

for run in $runs ; do 
    echo "Process ${run}"
    
    inFile="${baseDir}/picoDsts_${run}.in"

    if [ -f ${inFile} ] ; then
	rm ${inFile}
    fi

    while read -r line ; do

        # -- pick proper production
        echo $line | grep -v ${production} > /dev/null
        if [ $? -eq 0 ] ; then
            continue
        fi

	echo "${projectBaseDir}/${line}" >> ${inFile}

    done < <(find ${run} -name "*.picoDst.root" )
done 

popd > /dev/null




