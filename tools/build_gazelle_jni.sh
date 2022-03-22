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
if [ "$DO_CLEAN" == "clean" ]; then
    mvn clean $BATCH_MODE -P full-scala-compiler -Dbuild_arrow=${BUILD_ARROW} -Dbuild_cpp=ON -Dclean_cpp=${CLEAN_CPP} -DskipTests -Dcheckstyle.skip
fi

mvn package $BATCH_MODE -P full-scala-compiler -Dbuild_arrow=${BUILD_ARROW} -Dbuild_cpp=ON -Dclean_cpp=${CLEAN_CPP} -DskipTests -Dcheckstyle.skip

artifacts_dir=artifacts
mkdir -p $artifacts_dir
cp jvm/target/*.jar $artifacts_dir
cp cpp/build/src/plan_tester/plan_tester $artifacts_dir
cp -r tests $artifacts_dir
cp xiphos-spark-integration/resources/config.yaml $artifacts_dir
cp xiphos-spark-integration/xiphos-tools/build/table_info/table_info $artifacts_dir
cp xiphos-spark-integration/xiphos-tools/build/data_ingestion/parquet/xiphos-parquet-loader $artifacts_dir   
cp -r --preserve=links cpp/build/releases/* $artifacts_dir/
popd 
