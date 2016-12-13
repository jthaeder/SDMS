# SDMS
STAR Data Management Service

## Introduction
A data management service for STAR data stored and used at the NERSC facilities

## Sub Modules

Scripts are called in daily cron job: `cronSDMS.sh`

### CRON scripts
Scripts to crawl HPSS and populate mongoDB as 'the truth'.

#### Components
* `crawlerHPSS.py` - *Script to crawl over HPSS files*  
  [Read more here](README_CrawlerHPSS.md)

* `inspectHPSS.py` - *Script to check the filled HPSS mongoDB collections*  
  [Read more here](README_CrawlerHPSS.md)

* `processXRD.py` - *Scripts to process the output of the `crawlerXRD.py`*
  [Read more here](README_ProcessXRD.md)

## tarToHPSS
Scripts to move copy picoDsts to HPSS in tar files on a production day basis

[Read more here](tarToHPSS/ReadMe.md)
