# SDMS
STAR Data Management Service

## Introduction
A data management service for STAR data stored and used at the NERSC facilities

## Components

### HPSS Crawler



### tarToHPSS
Scripts to move copy picoDsts to HPSS in tar files on a production day basis

#### submitTarJobs.sh
Creates the list of 'days' to be copied and submits jobs

#### tarToHPSS.csh
Actual htar called by submitted jobs

#### checkStoredFiles.sh
Run afterwards to check if all tar files have been created 