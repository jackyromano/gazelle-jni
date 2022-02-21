#!/bin/bash

set -e

mydir=$(realpath $(dirname $0))
root=$mydir/..

function test_xiphos_datasource () {
    cd $root/xiphos-spark-integration/xiphos-datasource
    mvn test
    cd $root
}

test_xiphos_datasource

# basic plan_tester test to ensure that it doesn't crash on simple test
$root/cpp/build/src/plan_tester/plan_tester $root/tests/plan1.substrait

echo "Tests PASSED"
exit 0
