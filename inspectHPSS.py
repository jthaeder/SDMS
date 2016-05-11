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

# ____________________________________________________________________________
def inspector(collHpssFiles):
    """Check if all files are still on HPSS."""
    
    nDaysAgo = (datetime.date.today() - datetime.timedelta(days=N_DAYS_AGO)).strftime('%Y-%m-%d')

    lostPicoDsts = list(collHpssFiles.find({'lastSeen': {"$lt" : nDaysAgo}}))
    if lostPicoDsts:
        print("These files (", len(lostPicoDsts),") have not been seen for the last", N_DAYS_AGO,"days!")
        
        for entry in lostPicoDsts:
            print("  ", entry['fileFullPath'], "- last seen:", entry['lastSeen'])

# ____________________________________________________________________________
def printListOfUniqEntries(coll, field):
    """Print uniq entries in field."""

    column = list(coll.find({}, {field: True, '_id': False}))

    if '.' in field:
        fields = field.split('.')
        key = fields[1]
        uniqList = [{fields[1]: uniqValue} for uniqValue in set(item[fields[0]][fields[1]] for item in column)]
    else:
        key = field
        uniqList = [dict(tupleized) for tupleized in set(tuple(item.items()) for item in column)]  
      
    print('==========================================')    
    print('Collection:', coll.name, '\nUnique Entries in field:', field)
    for item in uniqList:
        print (item[key], " -> ", coll.find({field: item[key]}).count())

# ____________________________________________________________________________
def getListOfUniqEntries(coll, field):
    """Print uniq entries in field."""

    column = list(coll.find({}, {field: True, '_id': False}))

    if '.' in field:
        fields = field.split('.')
        uniqList = [{fields[1]: uniqValue} for uniqValue in set(item[fields[0]][fields[1]] for item in column)]
    else:
        uniqList = [dict(tupleized) for tupleized in set(tuple(item.items()) for item in column)]  
      
    return uniqList


# ____________________________________________________________________________
def main():
    """initialize and run"""

    # -- Connect to mongoDB
    dbUtil = mongoDbUtil("", "admin")

#    dbUtil.dropCollection("HPSS_Files")
#    dbUtil.dropCollection("HPSS_PicoDsts")
#    dbUtil.dropCollection("HPSS_Duplicates")

    collHpssFiles      = dbUtil.getCollection("HPSS_Files")
    collHpssPicoDsts   = dbUtil.getCollection("HPSS_PicoDsts")
    collHpssDuplicates = dbUtil.getCollection("HPSS_Duplicates")

    # -- General Info
    print(">>>", collHpssFiles)
    print("    Number of documents:", collHpssFiles.count())
    print(">>>", collHpssPicoDsts)
    print("    Number of documents:", collHpssPicoDsts.count())
    print(">>>", collHpssDuplicates)
    print("    Number of documents:", collHpssDuplicates.count())
   
    # -- Check if all files are still on HPSS
    #    inspector(collHpssFiles)


    #    for c in collHpssPicoDsts.find({'starDetails.trigger' : 'preview2'},  {'filePath': True, 
    for c in collHpssPicoDsts.find({'starDetails.production' : '040'},  {'filePath': True, 
                                                                         'starDetails.runnumber': True, 
                                                                         'starDetails.stream': True, 
                                                                         '_id': False}):
        print(c)
        
    print(collHpssPicoDsts.find({'starDetails.runnumber' : '13040003'}).count())
    """
    
    
    for ii in collHpssPicoDsts.find({'starDetails.energy' : '200GeV',
    'starDetails.system' : 'pp',
    'starDetails.runnumber' : '13040003'},
    {'filePath': True, 
    'starDetails.runnumber': True, 
    'starDetails.stream': True, 
    '_id': False}):
    print(ii)
    return
    """

    for ii in collHpssPicoDsts.find({'isInRunBak': True}):
        print(ii)

    for ii in collHpssPicoDsts.find({'fileFullName': { '$regex': '*.bak.*'}}):
        print(ii)

    # -- look at the columns
    printListOfUniqEntries(collHpssFiles, 'fileType')

    printListOfUniqEntries(collHpssPicoDsts, 'starDetails.runyear')
    printListOfUniqEntries(collHpssPicoDsts, 'starDetails.system')
    printListOfUniqEntries(collHpssPicoDsts, 'starDetails.energy')

    printListOfUniqEntries(collHpssPicoDsts, 'starDetails.trigger')
    printListOfUniqEntries(collHpssPicoDsts, 'starDetails.production')

#    print(getListOfUniqEntries(collHpssPicoDsts, 'starDetails.day'))
#    print(getListOfUniqEntries(collHpssPicoDsts, 'starDetails.runnumber'))

#    print(getListOfUniqEntries(collHpssPicoDsts, 'starDetails.stream'))
#    print(getListOfUniqEntries(collHpssPicoDsts, 'starDetails.picoType'))




    dbUtil.close()
# ____________________________________________________________________________
if __name__ == "__main__":
    print("Start HPSS Inspect!")
    sys.exit(main())
