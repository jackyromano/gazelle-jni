#!/bin/bash

mydir=$(dirname $0)
root=$mydir/..
do_clean="clean"
do_clean=""

BUILD_ARROW=ON
CLEAN_CPP=ON
DO_CLEAN=clean
BATCH_MODE=

for arg in "$@"
do
    case $arg in
        -p|--partial-build)
            DO_CLEAN=
            BUILD_ARROW=OFF
            CLEAN_CPP=OFF
            shift
            ;;
        -b|--batch)
            BATCH_MODE="-B"
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [-p]"
            echo "    -p : partial incremental build, no cleanup"
            exit 0
            shift
            ;;
    esac
done


pushd $root
mvn $DO_CLEAN package $BATCH_MODE -P full-scala-compiler -Dbuild_arrow=${BUILD_ARROW} -Dbuild_cpp=ON -Dclean_cpp=${CLEAN_CPP} -DskipTests -Dcheckstyle.skip
popd 
