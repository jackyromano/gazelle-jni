package com.intel.dbio.sources.datasourcev2.xiphosv2

import org.apache.spark.sql.SparkSession

import java.lang.Thread.sleep

object ThreadingSample {
  val datasourceName = "com.intel.dbio.sources.datasourcev2.xiphosv2"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[50]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    filterQuery(spark)
    //joinQuery(spark)
    sleep(50000000)
  }

  private def filterQuery(spark: SparkSession) = {
    val table1 = spark
      .read
      .format(datasourceName)
      .option("batch_size", "1024")
      .option("num_partitions", "12")
      .load("test_table_1")
    table1.createTempView("test_table_1")
    //val df = spark.sql("select * from test_table_1 where id > 10 and id < 20")
    val df = spark.sql("select * from test_table_1")
    println(df.count)
  }

  private def joinQuery(spark: SparkSession) = {
    val table1 = spark
      .read
      .format(datasourceName)
      .option("batch_size", "1024")
      .option("num_partitions", "1")
      .load("test_table_1")
    table1.createTempView("test_table_1")

    val table2 = spark
      .read
      .format(datasourceName)
      .option("batch_size", "2048")
      .option("num_partitions", "2")
      .load("test_table_2")
    table2.createTempView("test_table_2")

    val query = spark.sql(
      """
      select t1.value, t2.second_value, (t1.id + 1)
      from test_table_1 t1 join test_table_2 as t2 on t1.id = t2.id
      where t2.second_value > 2 and t2.second_value < 5 and t1.id > 2 and t1.id < 5
      order by t2.second_value
      """)
    query.explain
    println(query.count())
  }
}
