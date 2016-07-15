#!/bin/bash
#
# Go over list of picoDst files are check if they are 
# copied correctly
#
# Author: Jochen Thaeder                                           

baseDir=/global/homes/j/jthaeder/SDMS/diskToXRootD

############################################################

pushd $baseDir > /dev/null

input=$1
#input=picoDsts_Run14.in

inputFolder=`dirname $input`

# -- Prepare output 
mkdir -p results/$inputFolder

outputOK=results/${input}.ok
outputMissingMendel=results/${input}.missing.mendel
outputWrongSize=results/${input}.wrongSize
outputWrongSizeTxt=results/${input}.wrongSize.txt

rm -f ${outputMissingMendel} ${outputWrongSize} ${outputWrongSizeTxt} ${outputOK}
touch  ${outputMissingMendel} ${outputWrongSize} ${outputWrongSizeTxt} ${outputOK}

total=`cat $input | wc -l`
count=0

# -- Loop over inList
while read -r line ; do 

    echo "$count/ $total"
    let count=count+1

    tmp=`echo $line | awk -F"picodsts" '{ print $2 }'`
    path="/star/picodsts${tmp}"

    inSize=`stat -c %s ${line}`
    
    # -- Query XRD and parse output
    query=`${baseDir}/check/sQuery.sh $path`

    echo $query

    qCount=`echo $query | cut -d':' -f1`
    qSize=`echo $query | cut -d':' -f2`
    qIsOnMendel=`echo $query | cut -d':' -f3`
    qIsOnPDSF=`echo $query | cut -d':' -f4`

    if [ $qCount -eq 0 ] ; then
        # -- Is not there
	echo $line >> ${outputMissingMendel}
    else
	# -- Is there, check consistency
	if [ ${qSize} -eq  ${inSize} ] ; then
	    echo $line >> ${outputOK}
	    if [ "$qIsOnMendel" = "1" ] ; then
		echo $line missing on mendel
#	    elif [ "$qIsOnPDSF" = "1" ] ; then
#		echo $line missing on pdsf
	    fi
	else
	    echo $line >> ${outputWrongSize}
	    echo "$line : disk: ${inSize} xrd: ${qSize}">> ${outputWrongSizeTxt}
	fi
    fi
done < <( cat $input)


popd > /dev/null