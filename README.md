# SDMS
STAR Data Management Service

## Introduction
A data management service for STAR data stored and used at the NERSC facilities

## Sub Modules
The different sub modules are listed here classified by the user and place
where they are run.

### MongoDB collections
* The HPSS related mongoDB collections are described [here](README_CrawlerHPSS.md#mongodb-collections)
* The XRD related mongoDB collections are described [here](README_XRD.md#mongodb-collections)

### CRON script: `cronSDMS.sh`
Script is called on daily basis as normal user.

#### Components
* `crawlerHPSS.py` - *Script to crawl over HPSS files and to populate mongoDB as
  'the truth'*  
  [Read more here](README_CrawlerHPSS.md)

* `inspectHPSS.py` - *Script to check the filled HPSS mongoDB collections*  
  [Read more here](README_CrawlerHPSS.md)

* `processXRD.py` - *Scripts to process the output of the `crawlerXRD.py`*  
  [Read more here](README_XRD.md#xrd-process)

### CRON script: `crawlerXRD.sh`
Script is called on daily basis on every data storage node.

#### Components
* `crawlerXRD.py` - *Script to run on data server node, crawling the XRD space*  
  [Read more here](README_XRD.md#xrd-crawler)

### CRON script: `cronXRDSDMS.sh`
Script is called as `starxrd` user on `pstarxrdr1`. Can be run several times a
day, but at least once per day.

#### Components
* `cleanXRD.py` - *Script to clean the processed collections of XRD targets*  
  [Read more here](README_XRD.md#xrd-process)

* `dataServerCheck.py` - *Script to check all data servers - at least once a day*  
  [Read more here](README_XRD.md#xrd-check)


## tarToHPSS
Scripts to move copy picoDsts to HPSS in tar files on a production day basis.  
[Read more here](tarToHPSS/ReadMe.md)
