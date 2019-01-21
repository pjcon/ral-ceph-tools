#!/bin/bash
if [ $# -ne 1 ]; then
   echo "Usage: $0 pool:object"
   exit 1
fi

IFS=: read -ra ary <<<"$1"
pool=${ary[0]}
objname=${ary[1]}
rados -p ${pool} getxattr ${objname}.0000000000000000 XrdCks.adler32 | 
  od --skip-bytes=32 -N 4 -x |
  sed -e '1s/0000040//; s/ //g; s/\(..\)\(..\)/\2\1/g; q'
