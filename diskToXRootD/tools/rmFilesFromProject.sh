#!/bin/bash



for file in `find ../results -name "*ok*"` ; do 
    ls -l $file

    xargs -a $file -d'\n' rm -f
done


