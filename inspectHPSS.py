#!/usr/bin/env python
b'This script requires python 3.4'


"""
bla

"""

import sys

from mongoUtil import mongoDbUtil

# -- Check for a proper Python Version
if sys.version[0:3] < '3.0':
    print ('Python version 3.0 or greater required (found: {0}).'.format(sys.version[0:5]))
    sys.exit(-1)


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

    print(">>>", collHpssFiles)
    print("    Number of documents:", collHpssFiles.count())
    print(">>>", collHpssPicoDsts)
    print("    Number of documents:", collHpssPicoDsts.count())
    print(">>>", collHpssDuplicates)
    print("    Number of documents:", collHpssDuplicates.count())
   

    dbUtil.close()
# ____________________________________________________________________________
if __name__ == "__main__":
    print("Start HPSS Inspect!")
    sys.exit(main())
