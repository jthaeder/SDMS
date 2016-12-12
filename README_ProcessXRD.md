# XRD Process

Scripts to process the output of the `crawlerXRD.py` scripts and update the
**`XRD_<baseColl[target]>`** collections.
A sub-module of the SDMS suite.




## Components
* `processXRD.py`      - *Script to process output of `crawlerXRD.py`*

### processXRD.py
Script to process the output of the `crawlerXRD.py` script
**`XRD_<baseColl[target]>_new`** and **`XRD_<baseColl[target]>_missing`** collections.
and update the **`XRD_<baseColl[target]>`** collections.

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












## MongoDB collections
For every target, a set of mongoDB collections exist:  

The basic **3** are described in detail [here](README_CrawlerXRD#mongodb-collections):
* **`XRD_<baseColl[target]>`**
* **`XRD_<baseColl[target]>_new`**
* **`XRD_<baseColl[target]>_missing`**

Here, 3 more are added:
* **`XRD_<baseColl[target]>_noLink`**
* **`XRD_<baseColl[target]>_noHPSS`**
* **`XRD_<baseColl[target]>_corrupt`**
