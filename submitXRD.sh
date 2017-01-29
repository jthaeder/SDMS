#!/bin/bash
#
# Submit XRD staging processes 
#
# ########################################
#
#
# ########################################


if [ $# -gt 0 ] ; then 
    nJobs=$1
else
    nJobs=1000
fi


mkdir -p logsC
echo "qsub -v CHOS=sl64 -o logsC -j y -l h_vmem=1G -t 1-$nJobs ~/SDMS/stagerSDMSXRD.sh "
qsub -v CHOS=sl64 -o logsC -j y -l h_vmem=3G -t 1-$nJobs ~/SDMS/stagerSDMSXRD.sh 

