#!/bin/bash

docker_server="dbio-runner-vm1.iil.intel.com:5000"
mydir=$(dirname $0)

if [ "$#" -eq "0" ]; then
    echo "Usage: $0 <docker name>"
    exit 1
fi 

docker_name=$1
user_name=$(whoami)



docker build --build-arg HOME=${HOME} \
    --build-arg USER_NAME=$user_name \
    --build-arg USER_ID=$(id -u $user_name) \
    -t ${docker_server}/${docker_name}:latest $mydir/dev_docker
docker push ${docker_server}/${docker_name}:latest
