#!/bin/bash

set -e


export MAVEN_OPTS="-Dhttp.proxyHost=proxy-chain.intel.com -Dhttp.proxyPort=911 -Dhttp.nonProxyHosts=”localhost|*.intel.com -Dhttps.proxyHost=proxy-chain.intel.com -Dhttps.proxyPort=912"

mydir=$(realpath $(dirname $0))
root=$mydir/..

export DAXL_CONFIG_FILE=$root/artifacts/config.yaml
#Update LD_LIBRARY_PATH so plan_tester pickes the colmunar_jni .so
export LD_LIBRARY_PATH=$root/artifacts
if [ -z $SPARK_HOME ]; then
  export SPARK_HOME=/opt/spark/spark-3.1.1-bin-hadoop2.7
fi

function test_xiphos_datasource () {
    cd $root/xiphos-spark-integration/xiphos-datasource
    mvn test
    cd $root
}

function reset_xiphos() {
  sudo nbinsight-reset-se -t
  /usr/neuroblade/nb-gilt/scripts/nbinsight-kill-gilt-service.sh
  nohup /usr/neuroblade/nb-gilt/scripts/nbinsight-run-gilt-service.sh &
}
function load_strs_table() {
  $root/artifacts/strs_ddl strs
  $root/artifacts/etl_csv $root/artifacts/strs.csv
}

function prep_artifacts() {
  chmod +x $root/artifacts/plan_tester
  chmod +x $root/artifacts/etl_csv
  chmod +x $root/artifacts/strs_ddl
}

function test_plan_tester() {
  $root/artifacts/plan_tester $root/tests/plan1.substrait
}

function test_spark() {
  $root/tools/jni_shell < $root/tests/test_strs.scala
}

prep_artifacts
reset_xiphos
load_strs_table
#test_xiphos_datasource
test_plan_tester
test_spark

echo "Tests PASSED"
exit 0
