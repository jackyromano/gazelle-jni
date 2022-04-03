#!/bin/bash

server_name=dbio-dev-build1.iil.intel.com:5000

if [ "$#" -ne "2" ]; then
    echo "Usage: $0 <docker base name> <spark port>"
    exit 1
fi
docker_basename=$1
docker_name=$server_name/${docker_basename}:latest
spark_port=$2


docker run -d --rm --name $docker_basename \
    -p ${docker_port}:4040 \
    -v /tmp/.X11-unix:/tmp/.X11-unix \
    -v ${HOME}:${HOME} \
    -v /opt/intellij:/opt/intellij  \
    $docker_name

    
