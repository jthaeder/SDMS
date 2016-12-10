# XRD Crawler

Scripts to crawl over XRD on every data server and fill mongoDB collections.
A sub-module of the SDMS suite.

All scripts described here have to be executed as `starxrd` user.

## MongoDB collections

For every target, a set of **3** mongoDB collections exist:  
(For a list of targets see below)

### **`XRD_<baseColl[target]>`**
The main collection for every target, eg. **`XRD_PicoDsts`**

#### A document
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
      "details" : ["mc0101"],
      "location" : "XRD",
      "nCopies" : NumberInt(1) } }
```

* `filePath`: *relative path to picoDst file* - ***relates to name in HPSS collection***
* `fileSize`: *size of picoDst file in bytes* - **(`NumberInt`)**
* `fileFullPath`: *full path of picoDst (on XRD disk)*
* `target`:   *target file type*  
* `starDetails`: **sub document:** *keywords to identify picoDst file. Sub document copied from HPSS collection entry*
  * `runyear`, `system`, `energy`, `trigger`,  `production`, `day` **(`NumberInt`)**, `runnumber`, `stream`, `picoType`

* `storage`: **sub document:** *storage location of target data file*
  * `details`: *list of nodes where data file is stored*
  * `location`: *location of data file, only* ***XRD*** *for now*
  * `nCopies`: *number of copies on location* - **(`NumberInt`)**

### **`XRD_<baseColl[target]>_new`**
The collection for every target (eg. **`XRD_PicoDsts_new`**) of files which are
new on that node and are not reflected in the main target collection
**`XRD_<baseColl[target]>`**.

The collection is processed centrally after all crawlers are done.  
[Read more here](README_ProcessXRD.md)

```javascript
FIX ME

doc = {'fileFullPath': os.path.join(root, fileName),
       'filePath': os.path.join(root[len(self._workDir)+1:], fileName),
       'storage': {'location': 'XRD',
                   'detail': self._nodeName,
                   'disk': ''},
       'target': target,
       'fileSize': -1}
```

### **`XRD_<baseColl[target]>_miss`**
The collection for every target (eg. **`XRD_PicoDsts_miss`**) of files which are
supposed to be on the node but are missing, according to the main target
collection **`XRD_<baseColl[target]>`**

The collection is processed centrally after all crawlers are done.  
[Read more here](README_ProcessXRD.md)

```javascript
FIX ME

doc = {'fileFullPath': os.path.join(root, fileName),
       'filePath': os.path.join(root[len(self._workDir)+1:], fileName),
       'storage': {'location': 'XRD',
                   'detail': self._nodeName,
                   'disk': ''},
       'target': target,
       'fileSize': -1}
```

### **`XRD_DataServers`**
Information of all nodes in **`cluster.env`** file, updated daily

```javascript
{ "_id" : ObjectId("57a2d3a38579684ab6c45586"),
  "nodeName" : "mc1526",
  "stateActive" : true,
  "totalSpace" : NumberLong(7444929003520),
  "usedSpace" : NumberLong(893526790144),
  "freeSpace" : NumberLong(6173221539840),  
  "lastCrawlerRun" : "2016-09-24",
  "setInactive" : NumberInt(-1),
  "lastSeen" : "-1" }
```

* `nodeName`: *actual name of the node*
* `stateActive`: *state of node* - **(`boolean`)**
* `totalSpace`: *total space on all disks* - **(`NumberLong`)**
* `usedSpace`: *used space on all disks*-  **(`NumberLong`)**
* `freeSpace`: *free space on all disks* - **(`NumberLong`)**
* `lastCrawlerRun`: *last time the crawlerXRD run YYYY-MM-DD*
* `setInactive`: ***NEEDED ???***  - **(`NumberInt`)**
* `lastSeen`: ***NEEDED ???***

## Components
* `crawlerXRD.py`      - *Script to run on data server node, crawling the XRD space*
* `crawlerXRD.sh`      - *Daily script (`CRON`) to run on data server node*

* `dataServerCheck.py` - *Daily script  ..*

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

#### Actions

##### Process data files on data server node
Get an updated list of actual target files on the node and compare to list of files
which are supposed to be on the node.

Get list of all target files on the actual node, which are supposed to be on
that node (`listOfFilesOnNode`), from the main target collection:
**`XRD_<baseColl[target]>`** (eg. **`XRD_PicoDsts`**).

Crawl over working folder (`<ROOTD_PREFIX>/<baseFolders[target]>`) and inspect
all files of type `target`:

* Check if file link to actual data file is ok
  * *if not*: add entry to **`XRD_<baseColl[target]>_miss`** using `'issue' = 'brokenLink'` and proceed to next file

* Check if file is supposed to be on the node in `listOfFilesOnNode`.
  * *if not*: add entry to **`XRD_<baseColl[target]>_new`**   

After the crawl is finished, add any remaining entries from files, which are
supposed to be on the node (in `listOfFilesOnNode`) but couldn't be found,
to **`XRD_<baseColl[target]>_miss`**.

##### Update server info
After the crawl has been done, the information of the data server is collected
and the entry for the node is updated

Updated information:
* `stateActive` - to `true`
* `totalSpace`, `usedSpace`, `freeSpace`:
* `lastCrawlerRun`

### crawlerXRD.sh
Daily `CRON` script, executed locally on every server node. It calls `crawlerXRD.py`
