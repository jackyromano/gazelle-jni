#!/bin/bash

server_name=dbio-runner-vm1.iil.intel.com:5000

if [ "$#" -ne "1" ]; then
    if [ -z $DEV_DOCKER ]; then
        echo "Usage: $0 <docker base name>"
        echo "Alternativley, you can set DEV_DOCKER env variable to point to your development docker"
        exit 1
    else
    docker_basename=$DEV_DOCKER
    fi
else
    docker_basename=$1
fi
docker_name=${server_name}/${docker_basename}:latest

docker exec -it $docker_basename /bin/bash
