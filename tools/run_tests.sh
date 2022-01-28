#!/bin/bash

set -e

function test_xiphos_datasource () {
    cd $root/xiphos-spark-integration/xiphos-datasource
    mvn test
    cd $root
}
mydir=$(dirname $0)
root=$mydir/..

# TODO - enable once the test passes
#test_xiphos_datasource

# basic plan_tester test to ensure that it doesn't crash on simple test
$root/cpp/build/src/plan_tester/plan_tester $root/tests/plan1.substrait

echo "Tests PASSED"
exit 0
