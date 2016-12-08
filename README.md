# SDMS
STAR Data Management Service

## Introduction
A data management service for STAR data stored and used at the NERSC facilities

## Sub Modules

Scripts are called in daily cron job: `cronSDMS.sh`

### HPSS Crawler
Scripts to crawl HPSS and populate mongoDB as 'the truth'.

#### Components
* crawlerHPSS.py      - *Daily script to crawl over HPSS files*
* inspectHPSS.py      - *Daily script to check the filled mongoDB collections*

[Read more here](README_CrawlerHPSS.md)

### tarToHPSS
Scripts to move copy picoDsts to HPSS in tar files on a production day basis

[Read more here](tarToHPSS/ReadMe.md)
