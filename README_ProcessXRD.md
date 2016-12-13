# XRD Process

Scripts to process the output of the `crawlerXRD.py` scripts and update the
**`XRD_<baseColl[target]>`** collections.
A sub-module of the SDMS suite.

## MongoDB collections
For every target, a set of mongoDB collections exist:  

The basic **4** are described in detail [here](README_CrawlerXRD#mongodb-collections):
* **`XRD_<baseColl[target]>`**
* **`XRD_<baseColl[target]>_new`**
* **`XRD_<baseColl[target]>_missing`**
* **`XRD_<baseColl[target]>_brokenLink`**

Here, 2 more are added:
* **`XRD_<baseColl[target]>_noHPSS`**
* **`XRD_<baseColl[target]>_corrupt`**

They follow the layout of **`XRD_<baseColl[target]>_new`**, as the documents
are just moved there.

### **`XRD_<baseColl[target]>_noHPSS`**
The collection for every target of files which can not be found in the
**`HPSS_<target>`** collection (identified via `filePath`).

The entries have to be handled manually and then deleted from the collection.

### **`XRD_<baseColl[target]>_corrupt`**
The collection for every target of files which have a different `'fileSize'` on XRD
and on HPSS, where the `'fileSize'` in the collection **`XRD_<baseColl[target]>`**
is identical to the HPSS one in **`HPSS_<target>`**.

The entries have to be handled manually and then deleted from the collection.

## Components
* `processXRD.py`      - *Script to process output of `crawlerXRD.py`*

### processXRD.py
Script to process the output of the `crawlerXRD.py` script
**`XRD_<baseColl[target]>_new`** and **`XRD_<baseColl[target]>_missing`** collections.
and update the **`XRD_<baseColl[target]>`** collections.

Processing is only running when no other processing job is executed.

#### Actions

##### Process entries of **`XRD_<baseColl[target]>_missing`**

Process all entries in **`XRD_<baseColl[target]>_missing`** collection, depending
on their `'issue'` field

* `'issue' : missing`  
  Remove document in collection **`XRD_<baseColl[target]>`** if only one copy of
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
