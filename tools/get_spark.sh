#!/bin/bash

mydir=$(realpath $(dirname $0))
root=$mydir/..
cwd=$PWD


/bin/rm -rf /tmp/spark_download
mkdir -p /tmp/spark_download
cd /tmp/spark_download
wget --show-progress http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz 
tar zxf spark-3.1.1-bin-hadoop2.7.tgz -C $root 
cd $cwd
rm -rf /tmp/spark_download
echo "Spark downloaded to $root"
echo "Use:"
echo "export SPARK_HOME=$root/spark-3.1.1-bin-hadoop2.7"

