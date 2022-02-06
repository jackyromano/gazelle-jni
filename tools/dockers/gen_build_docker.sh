#!/bin/bash
mydir=$(realpath $(dirname $0))

docker build -t dbio-runner-vm1.iil.intel.com:5000/gazelle-jni-ci-image:latest $mydir/build_docker
docker push dbio-runner-vm1.iil.intel.com:5000/gazelle-jni-ci-image:latest

