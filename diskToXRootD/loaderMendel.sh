#!/bin/bash -l

# A qsub job script to load files into xrootd.

#$ -m beas
#$ -M jochen@thaeder.de
#$ -j y
#$ -l h_vmem=2G
#$ -t 1-300

# print out info if running as a qsub job
if [ -n "${SGE_O_HOST}" ]; then
    echo "---- batch node info ----"
    echo "pwd: `pwd -P`"
    echo "uname -a: `uname -a`"

    echo "cat /etc/redhat-release: `cat /etc/redhat-release`"
    echo "lsb_release -a:"
    lsb_release -a
    echo

    echo "cat ~/.chos: `cat ~/.chos`"
    echo "ls -l /proc/chos/link: `ls -l /proc/chos/link`"

    echo "env:"
    env

    echo "modules:"
    module avail
    echo "---- batch node info ----"
fi


if [ "$CHOS" != "sl62" ] ; then
    echo "Wrong CHOS - should be 'sl62' is '${CHOS}'"
    exit 1
fi

module unload python
module use /common/star/pkg/Modules || exit $?
module load python pymongo star-dm-scripts xrootd || exit $?

echo "starting loader:"
loader --xrd-manager mc0101-ib --max-load-fails 1500
echo "loader finished: " $?
