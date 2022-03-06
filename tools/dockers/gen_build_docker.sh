#!/bin/bash
mydir=$(realpath $(dirname $0))

docker build -t dbio-dev-build1.iil.intel.com:5000/gazelle-jni-ci-image:latest $mydir/build_docker
docker push dbio-dev-build1.iil.intel.com:5000/gazelle-jni-ci-image:latest

