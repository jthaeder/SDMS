# HPSS Crawler

Scripts to crawl over HPSS and fill mongoDB collections. A sub-module of the SDMS suite.

---

## MongoDB collections

### HPSS_Files
A collection of all files within those HPSS folders.  
**This is the true representation on what is on tape.**  
Every time the crawler runs, it updates the lastSeen field

#### A document
```javascript
{'_id': ObjectId('5723e67af157a6a310232458'),
 'fileSize': '13538711552',
 'fileType': 'tar',
 'filesInTar': 23,
 'fileFullPath': '/nersc/projects/starofl/picodsts/Run10/AuAu/11GeV/all/P10ih/148.tar',
 'lastSeen': '2016-04-29'}
```

* `fileSize`: *size of file in bytes*
* `fileType`: `tar, idx, picoDst, other`
* `filesInTar`: *if `fileType` is tar file, number of picoDsts in file*
* `fileFullPath`: *full path of file in HPSS* - **Unique index**
* `lastSeen`: *last time seen*

### HPSS_PicoDsts
A collection of all picoDsts stored on HPSS. Either as direct file or inside a tar file.  
*Every picoDst should show up only once. Duplicate entries are caught separately (see below)*

#### A document
```javascript
{'_id': 'Run10/AuAu/11GeV/all/P10ih/149/11149081/st_physics_adc_11149081_raw_2520001.picoDst.root',
 'filePath': 'Run10/AuAu/11GeV/all/P10ih/149/11149081/st_physics_adc_11149081_raw_2520001.picoDst.root',
 'fileSize': '5103599',
 'fileFullPath': '/project/projectdirs/starprod/picodsts/Run10/AuAu/11GeV/all/P10ih/149/11149081/st_physics_adc_11149081_raw_2520001.picoDst.root',
 'target': 'picoDst',
 'isInTarFile': True,
 'fileFullPathTar': '/nersc/projects/starofl/picodsts/Run10/AuAu/11GeV/all/P10ih/149.tar',
 'starDetails': {'runyear': 'Run10',
                 'system': 'AuAu',
                 'energy': '11GeV',
                 'trigger': 'all',
                 'production': 'P10ih',
                 'day': 149,
                 'runnumber': 11149081,
                 'stream': 'st_physics_adc',
                 'picoType': 'raw'},
 'staging': {'stageMarkerXRD': False}}
```

* `_id` and `filePath`: *relativ path to picoDst file* - **Unique indices**
* `fileSize`: *size of picoDst file in bytes*
* `fileFullPath`: *full path of picoDst (from disk)* - **REALLY USED?????**
*  `target`: `'picoDst'``
*  `isInTarFile`: `boolean` *if picoDsts is inside a tar file*
*  '`fileFullPathTar`: *if picoDsts is inside a tar file, the full path of the tar file in HPSS*
* `starDetails`: **sub document:** *keywords to identify picoDst file, parsed from file path*  
  * `runyear`, `system`, `energy`, `trigger`,  `production`, `day`, `runnumber`, `stream`, `picoType`
* `staging`:  **sub document:** *list of boolean stage marker for different stage location, e.g. XRD:*
  * `stageMarkerXRD`

### HPSS_Duplicates
A collection of duplicated picoDsts on HPSS

***Documents in the collection have to be treated manually!***

* PicoDst files can be deleted from HPSS for clean up if not in tar file.
* Be careful while deleting.
* If deleted files are whole tar file or single files, they will be indicated by a not updated `lastSeen` field in the `HPSS_Files` collection.
* Those documents in in the `HPSS_Files` and in the `HPSS_Duplicates` can be deleted manually.

---

## Components
* crawlerHPSS.py      - *Daily script to crawl over HPSS files*
* inspectHPSS.py      - *Daily script to check the filled mongoDB collections*

#### crawlerHPSS.py
Crawler which runs over all HPSS picoDST folder for now and populates mongoDB collections.
