#!/bin/bash -l

module load python/3.4.3

source ~/bin/.setXRDMongoEnv.sh

python ~/SDMS/crawlerXRD.py
