#!/bin/bash

if [ $(uname -s) = 'Linux' ]
then
    # clear PageCache, dentries and inodes
    sync; echo 3 > /proc/sys/vm/drop_caches
else
    echo "$0: unsupported platform"
fi