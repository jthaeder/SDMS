#!/usr/bin/env python
b'This script requires python 3.4'

"""
Connect to NERSC mongoDB sever and returns handles to the 
different collections


Author: Jochen Thaeder <jmthader@lbl.gov>
"""

import sys, os, datetime
import pymongo
from pymongo import MongoClient     


##############################################
# -- GLOBAL CONSTANTS

MONGO_SERVER  = 'mongodb01.nersc.gov'
MONGO_DB_NAME = 'STAR_XROOTD'

ADMIN_USER    = 'STAR_XROOTD_admin'
READONLY_USER = 'STAR_XROOTD_ro'

COLLECTION_INDICES = {'HPSS_Files': 'fileFullPath', 'HPSS_PicoDsts': 'filePath', 'XRD_DataServers': 'nodeName',
                      'XRD_PicoDsts': 'filePath'}

##############################################

# -- Check for a proper Python Version
if sys.version[0:3] < '3.0':
    print ('Python version 3.0 or greater required (found: {0}).'.format(sys.version[0:5]))
    sys.exit(-1)
if pymongo.__version__[0:3] < '3.0':
    print ('pymongo version 3.0 or greater required (found: {0}).'.format(pymongo.__version__[0:5]))
    sys.exit(-1)


# ----------------------------------------------------------------------------------
class mongoDbUtil:
    """Class to connect to mongoDB and perform actions."""

    # _________________________________________________________
    def __init__(self, args, userSwitch = 'user'):
        self.args = args

        # -- Get the password form env
        if userSwitch == "admin":
            self.user = ADMIN_USER
            self.password = os.getenv('STAR_XROOTD_ad', 'empty')
        else:
            self.user = READONLY_USER
            self.password = os.getenv('STAR_XROOTD_ro', 'empty')
            
        if self.password == 'empty':
            print("Password for user {0} at database {1} has not been supplied".format(self.user, MONGO_DB_NAME))
            sys.exit(-1)

        self.today = datetime.datetime.today().strftime('%Y-%m-%d')
        
        # -- Connect
        self._connectDB()

    # _________________________________________________________
    def _connectDB(self):
        """Connect to the NERSC mongoDB using pymongo."""

        self.client = MongoClient('mongodb://{0}:{1}@{2}/{3}'.format(self.user, self.password, 
                                                                     MONGO_SERVER, MONGO_DB_NAME))
        self.db = self.client[MONGO_DB_NAME]
#        print ("Existing collections:", self.db.collection_names(include_system_collections = False))

    # _________________________________________________________
    def close(self):
        """Close conenction to the NERSC mongoDB using pymongo."""

        self.client.close()
        self.db = ""

    # _________________________________________________________
    def getCollection(self, collectionName = 'HPSS_Files'):
        """Get collection and set index."""

        collection = self.db[collectionName]
        try:
            collection.create_index([(COLLECTION_INDICES[collectionName], pymongo.ASCENDING)], unique=True)
        except KeyError:
            #print ("Warning: Collection", collectionName, "not known. Index not created.")
            pass

            
        return collection

    # _________________________________________________________
    def dropCollection(self, collectionName):
        """Drop collection."""

        self.db[collectionName].drop()

# ----------------------------------------------------------------------------------

# ____________________________________________________________________________
def main():
    """Initialize and run,"""

    print("mongoDbUtil main")


# ----------------------------------------------------------------------------------

if __name__ == "__main__":
    """Call main."""
    
    sys.exit(main())
