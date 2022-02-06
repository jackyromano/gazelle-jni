#!/bin/bash

server_name=dbio-runner-vm1.iil.intel.com:5000

if [ "$#" -ne "1" ]; then
    echo "Usage: $0 <docker base name>"
    exit 1
fi
docker_basename=$1
docker rm -f $docker_basename


    
