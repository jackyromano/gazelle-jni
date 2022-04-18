package com.intel.dbio.sources.datasourcev2.xiphosv2

import org.junit._
import Assert._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Test
class DatasourceTests {
  val datasourceName = "com.intel.dbio.sources.datasourcev2.xiphosv2"
  val gazelle_jni_jar = "/home/yromano/projects/jni3/jvm/target/gazelle-jni-jvm-1.2.0-snapshot-jar-with-dependencies.jar"
  def initSession: SparkSession = {
    val spark = SparkSession.builder().master("local[1]")
      .config("spark.driver.extraClassPath", gazelle_jni_jar)
      .config("spark.executor.extraClassPath",gazelle_jni_jar)
      .config("spark.driver.cores","1")
      .config("spark.executor.instances","12")
      .config("spark.executor.cores","6")
      .config("spark.executor.memory","20G")
      .config("spark.memory.offHeap.size","80G")
      .config("spark.task.cpus","1")
      .config("spark.locality.wait","0s")
      .config("spark.sql.shuffle.partitions","72")
      .config("spark.plugins","com.intel.oap.GazellePlugin")
      .config("spark.sql.sources.useV1SourceList","avro")
      .config("spark.jars", gazelle_jni_jar)
      .getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")
    spark
  }

  val spark : SparkSession = initSession

  @Test
  def testJniStrings : Unit = {
    val df = spark.read.format(datasourceName).load("strs1")
    df.createTempView("strs1")
    val query_df = spark.sql("select * from strs1")
    query_df.explain
    query_df.show(10)
  }

  @Test
  def testJniParquet: Unit = {
    val test_file = "../resources/test_strings.parquet"
    val df = spark.read.parquet(test_file)
    df.createTempView("test_strings")
    val query_df = spark.sql("select * from test_strings")
    query_df.show(10)
  }
}


