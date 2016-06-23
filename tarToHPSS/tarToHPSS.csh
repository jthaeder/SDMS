#!/bin/csh
#
# Make tar file with htar into HPSS
#
# Author: Jochen Thaeder 
#
# Arguments:
#  $1 tarFolder  -> /nersc/projects/starofl/picodsts/Run14/AuAu/200GeV/physics2/P15ic 
#  $2 tarFile    -> 076.tar 
#  $3 inFolder   -> /project/projectdirs/starprod/picodsts/Run14/AuAu/200GeV/physics2/P15ic/076 
#  $4 doneFile   -> /global/homes/j/jthaeder/SDMS/tarToHPSS/tar_done_Run14.list

set tarFolder=$1
set tarFile=$2
set inFolder=$3
set doneFile=$4

# -- make folder first
hsi -P mkdir -p ${tarFolder}
set ret=$?
if ( $ret != 0 ) then
    echo ${inFolder} >> ${doneFile}.fail
    exit
endif

# -- create tar file
htar cf ${tarFolder}/${tarFile} ${inFolder} 
set ret=$?

# -- fill lists of done or failed files
if ( $ret == 0 ) then
    echo ${inFolder} >> ${doneFile}
else
    echo ${inFolder} >> ${doneFile}.fail
endif
