#!/bin/sh
rm -rf data;sudo mkdir -p /mnt/data;sudo chmod a+w /mnt/data
mkdir -p /mnt/data/download;mkdir -p /mnt/data/local;ln -s /mnt/data/ ./data
cd ec2;python gen_files.py
