#!/bin/bash

# Run command witha arguments in CHOS sl62

list=$@

CHOS=sl64 chos /usr/bin/flock -n /dev/shm/blah /bin/bash -l $list

exit