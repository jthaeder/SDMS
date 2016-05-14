#!/usr/bin/env python
b'This script requires python 3.4'


"""
bla

"""

import sys
import datetime

from mongoUtil import mongoDbUtil

##############################################
# -- GLOBAL CONSTANTS

N_DAYS_AGO = 14

##############################################

# -- Check for a proper Python Version
if sys.version[0:3] < '3.0':
    print ('Python version 3.0 or greater required (found: {0}).'.format(sys.version[0:5]))
    sys.exit(-1)

# ----------------------------------------------------------------------------------
class hpssInspectUtil:
    """Class to inspect HPSS collections."""

    # _________________________________________________________
    def __init__(self, nDaysAgo):
        self._nDaysAgo = nDaysAgo
        self._fields = {1: 'starDetails.runyear', 2: 'starDetails.energy', 3: 'starDetails.system',
                        4: 'starDetails.trigger', 5: 'starDetails.production'}
        self._fieldsExtra = {1: 'starDetails.day', 2: 'starDetails.runnumber', 3: 'starDetails.stream', 4: 'starDetails.picoType',
                             5: 'isInRunBak', 6: 'isInTarFile'}
                        
    # _________________________________________________________
    def setCollections(self, collHpssFiles, collHpssPicoDsts, collHpssDuplicates):
        """Get collection from mongoDB."""
        
        self._collHpssFiles      = collHpssFiles
        self._collHpssPicoDsts   = collHpssPicoDsts
        self._collHpssDuplicates = collHpssDuplicates

    # ____________________________________________________________________________
    def generalInfo(self):
        """Print generalInfo."""

        print(">>>", self._collHpssFiles)
        print("    Number of documents:", self._collHpssFiles.count())
        print(">>>", self._collHpssPicoDsts)
        print("    Number of documents:", self._collHpssPicoDsts.count())
        print(">>>", self._collHpssDuplicates)
        print("    Number of documents:", self._collHpssDuplicates.count())

    # ____________________________________________________________________________
    def inspector(self):
        """Check if all files are still on HPSS."""
        
        nDaysAgo = (datetime.date.today() - datetime.timedelta(days=self._nDaysAgo)).strftime('%Y-%m-%d')
        
        lostPicoDsts = list(self._collHpssFiles.find({'lastSeen': {"$lt" : nDaysAgo}}))
        if lostPicoDsts:
            print("These files (", len(lostPicoDsts),") have not been seen for the last", self._nDaysAgo,"days!")
            
            for entry in lostPicoDsts:
                print("  ", entry['fileFullPath'], "- last seen:", entry['lastSeen'])

    # ____________________________________________________________________________
    def printOverviewPicoDst(self):
        """Print overview of picoDsts."""

        print('\n==---------------------------------------------------------==')    
        print('Collection:',  self._collHpssPicoDsts.name)
               
        self._printOverviewLevelPicoDst(self._collHpssPicoDsts, 1, {})

    # ____________________________________________________________________________
    def printOverviewDuplicates(self):
        """Print overview of duplicate picoDsts."""

        print('\n==---------------------------------------------------------==')    
        print('Collection:',  self._collHpssDuplicates.name)
               
        self._printOverviewLevelPicoDst(self._collHpssDuplicates, 1, {})
  
    # ____________________________________________________________________________
    def _printOverviewLevelPicoDst(self, coll, level, queryOld):
        """ Print picoDst detqils recursivly for various depth."""
        
        try:
            field = self._fields[level]
        except:
            return
        
        for item in coll.distinct(field, queryOld):
            query = dict(queryOld)
            query[field] = item
            print ("    "*level, item, " -> ", coll.find(query).count())
            
            self._printOverviewLevelPicoDst(coll, level+1, query)


    # ____________________________________________________________________________
    def printDistinct(self):
        """Print list of distict values in fields."""

        print('\n==---------------------------------------------------------==')    
        print('Collection:',  self._collHpssFiles.name)
        print('==---------------------------------------------------------==')    
               
        self._printListOfUniqEntries(self._collHpssFiles, 'fileType')

        print('\n==---------------------------------------------------------==')    
        print('Collection:',  self._collHpssPicoDsts.name)
        print('==---------------------------------------------------------==')    

        for key, value in self._fields.items():
            self._printListOfUniqEntries(self._collHpssPicoDsts, value)

        for key, value in self._fieldsExtra.items():
            if key == 1:
                print('    Unique Entries in field:', value)
                print ('        ', sorted(self._collHpssPicoDsts.distinct(value)))
            elif key >= 3:
                self._printListOfUniqEntries(self._collHpssPicoDsts, value)
                    
#        print(list( self._collHpssPicoDsts.find({'isInRunBak': True})))

        print('\n==---------------------------------------------------------==')    
        print('Collection:',  self._collHpssDuplicates.name)
        print('==---------------------------------------------------------==')    

        for key, value in self._fields.items():
            self._printListOfUniqEntries(self._collHpssDuplicates, value)

        for key, value in self._fieldsExtra.items():
            if key == 1:
                print('    Unique Entries in field:', value)
                print ('        ', sorted(self._collHpssDuplicates.distinct(value)))
            elif key >= 3:
                self._printListOfUniqEntries(self._collHpssDuplicates, value)

    # ____________________________________________________________________________
    def _printListOfUniqEntries(self, coll, field):
        """Print uniq entries in field."""

        print('    Unique Entries in field:', field)
        for item in coll.distinct(field):
            print ('        ', item, " -> ", coll.find({field: item}).count())

    # ____________________________________________________________________________
    def compareDuplicates(self):
        """Compare duplicates in collection with entries in picoDsts."""

        with open("toBeDeletedx.txt", "w") as toBeDeleted:

            for duplicate in self._collHpssDuplicates.find({}):
                if duplicate['isInTarFile'] == True or duplicate['isInRunBak'] == False:
                    continue
                
                orig = self._collHpssPicoDsts.find_one({'filePath': duplicate['filePath']})
                
                if orig['fileSize'] != duplicate['fileSize']:
                    print('Is NOT equal: orig {0} - duplicate {1} : {2}'.format(orig['fileSize'],duplicate['fileSize'], duplicate['filePath']))
                    continue

                print(duplicate['fileFullPath'], file=toBeDeleted)
                self._collHpssDuplicates.delete_one({'_id': duplicate['_id']})


# ____________________________________________________________________________
def main():
    """initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

    collHpssFiles      = dbUtil.getCollection("HPSS_Files")
    collHpssPicoDsts   = dbUtil.getCollection("HPSS_PicoDsts")
    collHpssDuplicates = dbUtil.getCollection("HPSS_Duplicates")

    inspect = hpssInspectUtil(N_DAYS_AGO)
    inspect.setCollections(collHpssFiles, collHpssPicoDsts, collHpssDuplicates)

    # -- Print General Info
    inspect.generalInfo()

    # -- Check if all files are still on HPSS
#    inspect.inspector()

    # -- Print Overview picoDsts
#    inspect.printOverviewPicoDst()

    # -- Print Overview picoDsts - duplicates
    inspect.printOverviewDuplicates()

    # -- Print disinct fields
#    inspect.printDistinct()

    # -- Compare duplicates in duplicate collection
    inspect.compareDuplicates()


    dbUtil.close()
# ____________________________________________________________________________
if __name__ == "__main__":
    print("Start HPSS Inspect!")
    sys.exit(main())
