#!/bin/bash
mydir=$(realpath $(dirname $0))

docker_name=gazelle-jni-ci-image
if [ "$#" -eq "1" ]; then
    echo "set docker name to $1"
    docker_name=$1
fi
docker build -t dbio-dev-build1.iil.intel.com:5000/${docker_name}:latest $mydir/build_docker
docker push dbio-dev-build1.iil.intel.com:5000/${docker_name}:latest

