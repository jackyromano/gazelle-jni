#!/bin/bash

set -e


export MAVEN_OPTS="-Dhttp.proxyHost=proxy-chain.intel.com -Dhttp.proxyPort=911 -Dhttp.nonProxyHosts=”localhost|*.intel.com -Dhttps.proxyHost=proxy-chain.intel.com -Dhttps.proxyPort=912"

mydir=$(realpath $(dirname $0))
root=$mydir/..

export DAXL_CONFIG_FILE=$root/artifacts/config.yaml

function test_xiphos_datasource () {
    cd $root/xiphos-spark-integration/xiphos-datasource
    mvn test
    cd $root
}

test_xiphos_datasource

# basic plan_tester test to ensure that it doesn't crash on simple test
$root/artifacts/plan_tester $root/tests/plan1.substrait
# todo - add end-to-end gazelle tests

echo "Tests PASSED"
exit 0
