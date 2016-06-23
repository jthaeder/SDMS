#!/bin/bash
#
# Build lists of day folder - to be tar'ed and copied to HPSS
# and submit jobs which do it
#
# Author: Jochen Thaeder

basePath=/global/homes/j/jthaeder/SDMS/tarToHPSS
projectBaseDir=/project/projectdirs/starprod/picodsts
hpssBaseDir=/nersc/projects/starofl/picodsts

############################################################

# -- run year
runs="Run14"

# -- subset of production
production="200GeV/physics2/P15ic"

############################################################

pushd ${projectBaseDir} > /dev/null

for run in $runs ; do 

    # -- create log folder
    if [ -d ${basePath}/log.${run} ] ; then
	rm -rf ${basePath}/log.${run}
    fi
    mkdir -p ${basePath}/log.${run}

    # -- create report files
    doneFile=${basePath}/tar_done_${run}.list
    touch ${doneFile}
    touch ${doneFile}.fail

    # -- loop over days
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

	# -- build log path and creat it
	logFolder="${basePath}/log.${folder}"
	mkdir -p ${logFolder}

	# -- build in and out paths
	tarFile=${day}.tar
	tarFolder=${hpssBaseDir}/${folder}
	projectFolder=${projectBaseDir}/${line}
	
	options="-l starhpssio=1,h_cpu=24:00:00 -j y -o ${logFolder}/job.${day}.out"
	executable="${basePath}/tarToHPSS.csh"
	args="${tarFolder} ${tarFile} ${projectFolder} ${doneFile}"
	
	# -- submit jobs
	qsub ${options} ${executable} ${args}
	
    done < <(find ${run} -mindepth 5 -maxdepth 5 -type d)
done

popd > /dev/null
