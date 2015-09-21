#!/bin/sh
mkdir -p ${2}
for k in 1 2;
do for i in 1 2 4 8 16;
   do dd if=/dev/urandom of=${2}/${i}K${1}${k} bs=1K count=$i;
   done
done
