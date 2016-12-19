# Introduction

These scripts are used for checking the integrity of the XRD system. It is
divided in three different parts:

* [XRD Crawler](#xrd-crawler)  
  *Scripts to crawl over XRD on every data server and fill mongoDB collections.*

* [XRD Process](#xrd-process)  
  *Scripts to process the output of of the XRD crawler*

* [XRD Check](#xrd-check)  
  *Script to check the integrity of the data servers*

All XRD mongoDB collections are described below [here](#mongodb-collections)

# Parts

## XRD Crawler
Scripts to crawl over XRD on every data server and fill mongoDB collections.
A sub-module of the SDMS suite.

All scripts described here have to be executed as `starxrd` user.

### Components
* `crawlerXRD.py` - *Script to run on data server node, crawling the XRD space*
* `crawlerXRD.sh` - *Daily script (`CRON`) to run on data server node*

### crawlerXRD.py
Crawler which runs daily on all data server nodes and inspects the XRD space.
The XRD namespace can be found at `XROOTD_PREFIX = '/export/data/xrd/ns/star'`.
Findings are stored in mongoDB collections.

The files in the namespace are actually links with human readable names and
folder structure. The actual data files are saved with cryptical filenames on
different disks `DISK_LIST = ['data', 'data1', 'data2', 'data3', 'data4']`.

Different `targets` can be processed: `['picoDst', 'picoDstJet', 'aschmah']`  
Depending on the `target` different `baseFolders` and appropriate collection
names `baseColl` are selected.

```python
# -- possible targets
self._listOfTargets = ['picoDst', 'picoDstJet', 'aschmah']

# -- Base folders for targets
self._baseFolders = {'picoDst': 'picodsts',
                     'picoDstJet': 'picodsts/JetPicoDsts',
                     'aschmah': 'picodsts/aschmah'}

# -- Base collection names for targets
self._baseColl = {'picoDst': 'PicoDsts',
                  'picoDstJet': 'PicoDstsJets',
                  'aschmah': 'ASchmah'}
```
#### Collections
* read:
  * **`XRD_<baseColl[target]>`**

* read+write:
  * **`XRD_<baseColl[target]>_new`**
  * **`XRD_<baseColl[target]>_missing`**
  * **`XRD_<baseColl[target]>_brokenLink`**
  * **`XRD_DataServers`**

#### Actions

##### Process data files on data server node
Get an updated list of actual target files on the node and compare to list of files
which are supposed to be on the node.

Get list of all target files on the actual node, which are supposed to be on
that node (`listOfFilesOnNode`), from the main target collection:
**`XRD_<baseColl[target]>`** (eg. **`XRD_PicoDsts`**).

Crawl over working folder (`<XROOTD_PREFIX>/<baseFolders[target]>`) and inspect
all files of type `target`:

* Check if file link to actual data file is ok
  * *If not*: add entry to **`XRD_<baseColl[target]>_brokenLink`** using `'issue' = 'brokenLink'` and proceed to next file

* Check if file is supposed to be on the node in `listOfFilesOnNode`.
  * *If not*: add entry to **`XRD_<baseColl[target]>_new`**   

After the crawl is finished, add any remaining entries from files, which are
supposed to be on the node (in `listOfFilesOnNode`) but couldn't be found,
to **`XRD_<baseColl[target]>_missing`**.

##### Update server info
After the crawl has been done, the information of the data server is collected
and the entry for the node is updated

Updated information:
* `totalSpace`, `usedSpace`, `freeSpace`:
* `lastCrawlerRun` as *YYYY-MM-DD*

### crawlerXRD.sh
Daily `CRON` script, executed locally on every server node. It calls `crawlerXRD.py`

## XRD Process
Scripts to process the output of the `crawlerXRD.py` scripts and update the
**`XRD_<baseColl[target]>`** collections.

### Components
* `processXRD.py` - *Script to process output of `crawlerXRD.py`*
* `cleanXRD.py` - *Script to process output of `crawlerXRD.py` and `processXRD.py`*

### processXRD.py
Script to process the output of the `crawlerXRD.py` script
**`XRD_<baseColl[target]>_new`** and **`XRD_<baseColl[target]>_missing`** collections.
and update the **`XRD_<baseColl[target]>`** collections.

Processing is only running when no other processing job is executed.

#### Collections
* read:
  * **`HPSS_<baseColl[target]>`**

* read+write:
  * **`XRD_<baseColl[target]>`**
  * **`XRD_<baseColl[target]>_new`**
  * **`XRD_<baseColl[target]>_missing`**
  * **`XRD_<baseColl[target]>_noHPSS`**
  * **`XRD_<baseColl[target]>_corrupt`**

#### Actions

##### Process entries of **`XRD_<baseColl[target]>_missing`**
Process all entries in **`XRD_<baseColl[target]>_missing`** collection.

* Remove document in collection **`XRD_<baseColl[target]>`** if only one copy of
  of file is on disk (`doc['storage']['nCopies'] == 1`).

  If more the one copy is on disk, remove the appropriate entry(ies) in the set
  `doc['storage']['detail']` and decrement `doc['storage']['nCopies']` by one.

##### Process entries of **`XRD_<baseColl[target]>_new`**
Process all entries of the collection **`XRD_<baseColl[target]>_new`**
one-by-one, using the `'filePath'` field as index.

* Get a `list` of all entries in **`XRD_<baseColl[target]>_new`** with the
  same `'filePath'`. Compare their `'fileSize'`s. If there are not all the
  same, use only the current entry (reduce the list to the document).

* Get a `set` of all node names within list off all entries with the same
  `'filePath'` (= `nodeSet`). This handles multiple entries of the same
  `'filePath'` with different nodes and/or duplicates.

* Get a `dict` of all node name and disk pairs (=`nodeDiskDict`)

* If there is already is an existing document of `'filePath'` in the target
  collection **`XRD_<baseColl[target]>`**:

  * Check if `'fileSize'` is of the document in **`XRD_<baseColl[target]>`**
    and **`XRD_<baseColl[target]>_new`** equal:
      * If not move new document to collection **`XRD_<baseColl[target]>_corrupt`**
        and remove document from collection **`XRD_<baseColl[target]>_new`**
        (`continue`).

  * Update the field `doc['storage']['details']` with the `nodeSet` and
    `doc['storage']['disks']` with the `nodeDiskDict` in **`XRD_<baseColl[target]>`**.

* If document there isn't one of `'filePath'` in the target collection
  **`XRD_<baseColl[target]>`**:

  * Get hpss document from **`HPSS_<baseColl[target]>`**:

    * If there is no document of `'filePath'` in the **`HPSS_<baseColl[target]>`**,
      move new document to collection **`XRD_<baseColl[target]>_noHPSS`** and
      remove document from collection **`XRD_<baseColl[target]>_new`**
      (`continue`).

  * Create new document for **`XRD_<baseColl[target]>`** from hpss document and from
    content of new documents

  * Check `'fileSize'`s of XRD entry/ies compared to the HPSS entry. If they
    are equal, just add a new document to the collection **`XRD_<baseColl[target]>`**.
    Otherwise the new documents are moved to collection of corrupt documents
    **`XRD_<baseColl[target]>_corrupt`**.

### cleanXRD.py
  Script to process the output of the `crawlerXRD.py` and `processXRD.py` scripts.
  Cleans XRD from broken links and corrupt files in **`XRD_<baseColl[target]>_corrupt`**
  and **`XRD_<baseColl[target]>_brokenLink`** collections.

  To be executed as `starxrd` user, preferably as cron job in `cronXRDSDMS.sh`.

#### Collections
  * read:
    * **`HPSS_<baseColl[target]>`**
    * **`XRD_<baseColl[target]>`**

  * read+write:
    * **`XRD_<baseColl[target]>_noHPSS`**
    * **`XRD_<baseColl[target]>_corrupt`**

#### Actions

##### Process broken links
Process all entries of the collection **`XRD_<baseColl[target]>_brokenLink`**
one-by-one, using the `'nodeFilePath'` field as index.

* Sort all broken link files per node.
* Delete all file with broken links on the same nodes at once, using `ssh <nodeName> rm -f <fileList>`

##### Process corrupt files
Process all entries of the collection **`XRD_<baseColl[target]>_corrupt`**
one-by-one, using the `'nodeFilePath'` field as index.

* Delete all files with `fileSize = 0` using `xrd <servername> rm <file>`
* Other files are printed out, so that they can be checked by hand.

## XRD Check


# MongoDB Collections

## **`XRD_<baseColl[target]>`**
The main XRD mongoDB collection, which is used as reference of what is stored where.
One for every target exists, eg. **`XRD_PicoDsts`**.

```javascript
{ "_id" : ObjectId("57e6dbc9b74c2f221552a644"),
  "filePath" : "Run14/AuAu/200GeV/physics2/P15ic/102/15102060/st_physics_adc_15102060_raw_5000009.picoDst.root",
  "fileSize" : NumberInt(34341663),
  "fileFullPath" : "/export/data/xrd/ns/star/picodsts/Run14/AuAu/200GeV/physics2/P15ic/102/15102060/st_physics_adc_15102060_raw_5000009.picoDst.root",
  "target" : "picoDst",
  "starDetails" : {
      "runyear" : "Run14",
      "system" : "AuAu",
      "energy" : "200GeV",
      "trigger" : "physics2",
      "production" : "P15ic",
      "day" : NumberInt(102),  
      "runnumber" : "15102060",
      "stream" : "st_physics_adc",
      "picoType" : "raw" },
  "storage" : {
      "details" : ["mc1313", "mc0129"],
      "disks" : {"mc1313" : "data", "mc0129" : "data1"},
      "location" : "XRD",
      "nCopies" : NumberInt(2) } }
```

* `filePath`: *relative path to picoDst file* - **Unique index** - ***relates to name in HPSS collection***
* `fileSize`: *size of picoDst file in bytes* - **(`NumberInt`)**
* `fileFullPath`: *full path of picoDst (on XRD disk)*
* `target`:   *target file type*  
* `starDetails`: **sub document:** *keywords to identify picoDst file. Sub document copied from HPSS collection entry*
  * `runyear`, `system`, `energy`, `trigger`,  `production`, `day` **(`NumberInt`)**, `runnumber`, `stream`, `picoType`
* `storage`: **sub document:** *storage location of target data file*
  * `details`: *list of nodes where data file is stored*
  * `location`: *location of data file, only* ***XRD*** *for now*
  * `nCopies`: *number of copies on location* - **(`NumberInt`)**
  * `disks`: *disk location where copies are stored - **(as `detail:disk`)**

## **`XRD_<baseColl[target]>_<output_states>`**
  There are **2** collections which are used to process the `crawlerXRD.py` output
  by `processXRD.py`

  * **`XRD_<baseColl[target]>_new`**
  * **`XRD_<baseColl[target]>_missing`**

  They have the same layout:
  ```javascript
  {
      "_id" : ObjectId("585007577a1dbe1e37327b51"),
      "fileSize" : NumberInt(678068224),
      "filePath" : "Run12/pp/200GeV/all/P12id/064/13064055/st_physics_13064055_merged_5.picoDst.root",
      "target" : "picoDst",
      "fileFullPath" : "/export/data/xrd/ns/star/picodsts/Run12/pp/200GeV/all/P12id/064/13064055/st_physics_13064055_merged_5.picoDst.root",
      "storage" : {
          "detail" : "mc1231",
          "location" : "XRD",
          "disk" : "data"} }
  ```

  * `filePath`: *relative path to picoDst file* - ***relates to name in HPSS collection***
  * `fileSize`: *size of picoDst file in bytes* - **(`NumberInt`)**
  * `target`:   *target file type*  
  * `fileFullPath`: *full path of picoDst (on XRD disk)*
  * `storage`: **sub document:** *storage location of target data file*
    * `detail`: *node where data file is stored*
    * `location`: *location of data file, only* ***XRD*** *for now*
    * `disks`: *disk location*

### **`XRD_<baseColl[target]>_new`**
A collection for every target (eg. **`XRD_PicoDsts_new`**) of files which are
new on that node and are not reflected in the main target collection
**`XRD_<baseColl[target]>`**.

The collection is processed centrally after all crawlers are done by `processXRD.py`.

### **`XRD_<baseColl[target]>_missing`**
The collection for every target (eg. **`XRD_PicoDsts_missing`**) of files which are
supposed to be on the node but are missing, according to the main target
collection **`XRD_<baseColl[target]>`**

The collection is processed centrally after all crawlers are done by `processXRD.py`.


##  **`XRD_<baseColl[target]>_<process_states>`**
There are **3** collections which are used to process the `crawlerXRD.py` output
by `processXRD.py` and `cleanXRD.py`.

* **`XRD_<baseColl[target]>_brokenLink`**
* **`XRD_<baseColl[target]>_corrupt`**
* **`XRD_<baseColl[target]>_noHPSS`**

They have the same layout:
```javascript
{
    "_id" : ObjectId("585007577a1dbe1e37327b51"),
    "fileSize" : NumberInt(678068224),
    "filePath" : "Run12/pp/200GeV/all/P12id/064/13064055/st_physics_13064055_merged_5.picoDst.root",
    "nodeFilePath" : "mc1231_Run12/pp/200GeV/all/P12id/064/13064055/st_physics_13064055_merged_5.picoDst.root",
    "target" : "picoDst",
    "fileFullPath" : "/export/data/xrd/ns/star/picodsts/Run12/pp/200GeV/all/P12id/064/13064055/st_physics_13064055_merged_5.picoDst.root",
    "storage" : {
        "detail" : "mc1231",
        "location" : "XRD",
        "disk" : "data"} }
```

* `nodeFilePath`: *combination of detail and filePath* - **Unique index**
* `fileSize`: *size of picoDst file in bytes* - **(`NumberInt`)**
* `filePath`: *relative path to picoDst file* - ***relates to name in HPSS collection***
* `target`:   *target file type*  
* `fileFullPath`: *full path of picoDst (on XRD disk)*
* `storage`: **sub document:** *storage location of target data file*
  * `detail`: *node where data file is stored*
  * `location`: *location of data file, only* ***XRD*** *for now*
  * `disks`: *disk location*

### **`XRD_<baseColl[target]>_brokenLink`**
The collection for every target (eg. **`XRD_PicoDsts_brokenLink`**) of files
which are supposed to be on the node but only the link exists (and not the
actual data file), according to the main target collection
**`XRD_<baseColl[target]>`**.

Broken Links on disk from **`XRD_<baseColl[target]>_brokenLink`** are deleted
from cron script `cleanXRD.py` running as `starxrd` user.

### **`XRD_<baseColl[target]>_corrupt`**
The collection for every target of files which have a different `'fileSize'` on XRD
and on HPSS, where the `'fileSize'` in the collection **`XRD_<baseColl[target]>`**
is identical to the HPSS one in **`HPSS_<target>`**.

The entries mostly have to be handled manually and then deleted from the collection (for now).
The files with size 0 are deleted by `cleanXRD.py`.

### **`XRD_<baseColl[target]>_noHPSS`**
The collection for every target of files which can not be found in the
**`HPSS_<target>`** collection (identified via `filePath`).

The entries have to be handled manually and then deleted from the collection.

## **`XRD_DataServers`**
Information of all nodes in **`cluster.env`** file, updated daily

```javascript
{ "_id" : ObjectId("57a2d3a38579684ab6c45586"),
  "nodeName" : "mc1526",
  "stateActive" : true,
  "totalSpace" : NumberLong(7444929003520),
  "usedSpace" : NumberLong(893526790144),
  "freeSpace" : NumberLong(6173221539840),  
  "lastCrawlerRun" : "2016-09-24",
  "lastSeen" : "2016-09-24" }
```

* `nodeName`: *actual name of the node* - **Unique index**
* `stateActive`: *state of node* - **(`boolean`)**
* `totalSpace`: *total space on all disks* - **(`NumberLong`)**
* `usedSpace`: *used space on all disks*-  **(`NumberLong`)**
* `freeSpace`: *free space on all disks* - **(`NumberLong`)**
* `lastCrawlerRun`: *last time the crawlerXRD run YYYY-MM-DD*
* `lastSeenActive`: *last time a node as seen active YYYY-MM-DD*
