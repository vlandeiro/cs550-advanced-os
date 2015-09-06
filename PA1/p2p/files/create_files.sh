#!/bin/sh
for i in 1 2 4 8 16;
do dd if=/dev/urandom of=file_${i}K_${1} bs=1K count=$i;
done
