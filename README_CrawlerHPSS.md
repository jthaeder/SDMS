# HPSS Crawler

Scripts to crawl over HPSS and fill mongoDB collections. A sub-module of the SDMS suite.

## MongoDB collections

### HPSS_Files
A collection of all files within those HPSS folders.  
**This is the truth representation on what is on tape.**  
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

* `fileSize`: *size of file in bytes* - **(`NumberLong`)**
* `fileType`: `tar, idx, picoDst, other`
* `filesInTar`: *exists if `fileType` is tar file, number of picoDsts in file* **(`NumberInt`)**
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
* `fileSize`: *size of picoDst file in bytes* - **(`NumberInt`)**
* `fileFullPath`: *full path of picoDst (from disk)* - **REALLY USED?????**
*  `target`: `'picoDst'` *target file types*  
*  `isInTarFile`: `boolean` *if picoDsts is inside a tar file*
*  '`fileFullPathTar`: *if picoDsts is inside a tar file, the full path of the tar file in HPSS*
* `starDetails`: **sub document:** *keywords to identify picoDst file, parsed from file path*  
  * `runyear`, `system`, `energy`, `trigger`,  `production`, `day` **(`NumberInt`)**, `runnumber`, `stream`, `picoType`
* `staging`:  **sub document:** *list of boolean stage marker for different stage location, e.g. XRD:*
  * `stageMarkerXRD`

### HPSS_Duplicates
A collection of duplicated picoDst files on HPSS  
***Documents in this collection have to be treated manually!***

* The duplicated picoDst files can be deleted from HPSS  
  ***Be careful with deleting any files.***
  * If they are not in a tar file - just delete them  
  * If they are in a tar file - mark them as deleted them via `htar -Df`
* The corresponding documents in the collection **`HPSS_Duplicates`** have to be
  deleted manually.
* If the deleted files are whole tar files or single files, they will be indicated
  by a non updated `lastSeen` field in the **`HPSS_Files`** collection.
  * Those documents in the **`HPSS_Files`** collection have to be deleted manually.

## Components
* crawlerHPSS.py      - *Daily script to crawl over HPSS files*
* inspectHPSS.py      - *Daily script to check the filled mongoDB collections*

### crawlerHPSS.py
Crawler which runs over all HPSS picoDST folders and populates mongoDB
collections.

#### Actions

##### HPSS data consistency
Script crawls over a part of the HPSS space (`HPSS_BASE_FOLDER`/`[PICO_FOLDERS]`,
eg. `/nersc/projects/starofl/[picodsts,picoDST]`) and keeps a record of all files
in the **`HPSS_Files`** collection. These records are checked every time the
script runs and the `lastSeen` field gets updated to check HPSS data consistency.

This uses the `hsi -q` command to access HPSS and an recursive `ls -lR` to
minimize HPSS access. The output of these command are parsed line-by-line.  

##### *Truth* basis of data files of type target
All root files of the data type `target` (for now only `picoDst`) are also put
into an extra *target collection* (**`HPSS_<target>s`**, eg. **`HPSS_PicoDsts`**).
This collection builds the ***Truth*** basis for that data type. They are only
added once, when a new file is added to **`HPSS_Files`**.

Some files are tar files produced by `htar` and also contain root files. When a
new tar file is found, the file is opened and listed once via `har -tf` and the
output is parsed line-by-line. If an error occurs, the file is not added to
**`HPSS_Files`** and it has to be looked at by hand (maybe idx file is missing
or corrupt file). Otherwise, the contained root files of the data type `target`
are added to the *target collection*.

During the process of adding files to the *target collection*, it's relative path
`filePath` is parsed using the `pathKeysSchema` retrieving the `starDetails` to
identify the data set.

The relative path `filePath` is a unique index of this collection. The below for
handling of duplicates.

##### Duplicates
New data files to be added in the *target collection* which already exist, will
be added to to the **`HPSS_Duplicates`** collection. Documents in this collection
have to be treated manually!

### inspectHPSS.py
Checks the collections populated by `crawlerHPSS.py`. Several methods can be
turned on or off.

#### Methods
All the methods below are for the collections **`HPSS_Files`** and **`HPSS_PicoDsts`**.
The collection **`HPSS_Duplicates`** is only considered if it contains documents.

* **`generalInfo()`**  
  On **`HPSS_Files`**, **`HPSS_PicoDsts`**, (**`HPSS_Duplicates`**): Print general
  info of collections.

* **`inspector()`**  
  On **`HPSS_Files`**: check if all files are still on HPSS. The field `lastSeen`
  must younger then `N_DAYS_AGO = 14`.

* **`printOverviewPicoDst()`**  
  On **`HPSS_PicoDsts`**: print overview of picoDsts details recursively.

* **`printOverviewDuplicates()`**  
  On **`HPSS_Duplicates`**: print overview of duplicated picoDsts details
  recursively.

* **`printDistinct()`**  
  On **`HPSS_Files`**, **`HPSS_PicoDsts`**, (**`HPSS_Duplicates`**): print lists
  of distinct entries.

* **`compareDuplicates()`**  
  On **`HPSS_Duplicates`**: print comparison of duplicate entries with original
  entries.
