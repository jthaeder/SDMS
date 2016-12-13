# SDMS
STAR Data Management Service

## Introduction
A data management service for STAR data stored and used at the NERSC facilities

## Sub Modules
The different sub modules are listed here classified by the user and place
where they are run.

### CRON script: `cronSDMS.sh`
Script is called on daily basis as normal user.

#### Components
* `crawlerHPSS.py` - *Script to crawl over HPSS files and to populate mongoDB as
  'the truth'*  
  [Read more here](README_CrawlerHPSS.md)

* `inspectHPSS.py` - *Script to check the filled HPSS mongoDB collections*  
  [Read more here](README_CrawlerHPSS.md)

* `processXRD.py` - *Scripts to process the output of the `crawlerXRD.py`*  
  [Read more here](README_ProcessXRD.md)

### CRON script: `crawlerXRD.sh`
Script is called on daily basis on every data storage node.

#### Components
* `crawlerXRD.py` - *Script to run on data server node, crawling the XRD space*  
  [Read more here](README_CrawlerXRD.md)


## tarToHPSS
Scripts to move copy picoDsts to HPSS in tar files on a production day basis.  
[Read more here](tarToHPSS/ReadMe.md)
