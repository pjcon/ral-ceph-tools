#!/bin/bash

#pass file containing checksum 
csfile=$1

#echo 1
#cat $csfile
#echo -e "\n2"
#cat $csfile | od --skip-bytes=32 -N 4 -x 
#echo 3
adler32=$(cat $csfile | od --skip-bytes=32 -N 4 -x | sed -e '1s/0000040//; s/ //g; s/\(..\)\(..\)/\2\1/g; q')
echo $adler32

