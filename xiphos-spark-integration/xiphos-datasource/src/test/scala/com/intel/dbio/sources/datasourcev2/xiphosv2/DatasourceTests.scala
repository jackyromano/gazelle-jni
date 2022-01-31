package com.intel.dbio.sources.datasourcev2.xiphosv2

import org.junit._
import Assert._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Test
class DatasourceTests {
  val datasourceName = "com.intel.dbio.sources.datasourcev2.xiphosv2"
  def initSession: SparkSession = {
    val spark = SparkSession.builder().master("local[3]").getOrCreate()
    spark
  }
  val spark : SparkSession = initSession

  def testTable(batchSize : Int) : Unit = {
    val df = spark
      .read
      .format(datasourceName)
      .option("batch_size", batchSize.toString)
      .load("test_table_1")

    assertTrue(validateSchema(df.schema))
    assertTrue(validateFilter(df,  batchSize * 10))
  }

  @Test
  def testTable1(): Unit = {
    testTable(3)
    testTable(batchSize =  1024)
  }
  @Test
  def testTable2(): Unit = {
    val df = spark.read.format(datasourceName).load("test_table_2")
    df.show(10)
    assertTrue(df.schema.length == 3)
  }
  private def validateFilter(df: DataFrame, expectedItems : Int) : Boolean = {
    val filtered_df = df.where("value = \"2\"")
    filtered_df.explain()
    filtered_df.show(30)
    println(filtered_df.count)
    filtered_df.count == expectedItems // TODO - should be 1 if filter actually worked
  }

  def validateSchema(schema : StructType): Boolean = {
    var isSchemaOk : Boolean = schema.length == 2
    isSchemaOk = isSchemaOk && schema(0).name == "id"
    isSchemaOk = isSchemaOk && schema(0).dataType.getClass == IntegerType.getClass
    isSchemaOk = isSchemaOk && schema(1).name == "value"
    isSchemaOk = isSchemaOk && schema(1).dataType.getClass == StringType.getClass
    isSchemaOk
  }

  @Test
  def testXiphosMultipleTables() = {
    val table1 = spark.read.format(datasourceName).load("test_table_1")
    table1.createTempView("test_table_1")
    val table2 = spark.read.format(datasourceName).load("test_table_2")
    table2.createTempView("test_table_2")
    val query = spark.sql( """
      select t1.value, t2.second_value
      from test_table_1 t1 join test_table_2 as t2 on t1.id = t2.id
      where t2.second_value > 2 and t2.second_value < 5 and t1.id > 2 and t1.id < 5
      order by t2.second_value
      """)
    query.explain
    query.show()
  }
}


