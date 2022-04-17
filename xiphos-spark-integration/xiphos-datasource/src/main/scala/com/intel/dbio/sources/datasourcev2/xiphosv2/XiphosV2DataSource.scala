package com.intel.dbio.sources.datasourcev2.xiphosv2

import breeze.linalg.min

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import java.util
import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.datasources.v2.FileScan

//import com.intel.oap.execution.{NativeFilePartition, NativeSubstraitPartition}

import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}


class DefaultSource extends TableProvider {
  var verbose = false;

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    getTable(null,Array.empty[Transform],caseInsensitiveStringMap.asCaseSensitiveMap()).schema()
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {

    val batchSize = properties.getOrDefault("batch_size", "3").toInt
    if (verbose) {
      println("getTable properties:")
      properties.forEach((key: String, value: String) => {
        println("key: " + key + " Value: " + value)
      })
    }
    new XiphosV2BatchTable(properties)
  }
}

class XiphosV2BatchTable(val _properties : util.Map[String, String]) extends Table with SupportsRead {
  val tableName = _properties.get("path")
  override def name(): String = this.getClass.toString + "_" + tableName

  override def schema(): StructType = {
    XiphosJNI.getSchema(tableName)
  }

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new XiphosV2ScanBuilder(options)
  }
}

class XiphosV2ScanBuilder(options : CaseInsensitiveStringMap) extends ScanBuilder {

  override def build(): Scan = new XiphosV2Scan(options)
}

class XiphosV2Scan(val options: CaseInsensitiveStringMap) extends Scan with Batch {
  override def toBatch: Batch = this
  override def readSchema(): StructType = StructType(Array(StructField("value", StringType)))

  override def planInputPartitions(): Array[InputPartition] = {
    val batchSize = options.getOrDefault("batch_size", "3").toInt
    val n_partitions = options.getOrDefault("num_partitions", "1").toInt
    val tableName = options.get("path")
    var parts : Array[InputPartition] = Array.empty;
    for (i <- 0 until n_partitions) {
      parts = parts :+ new XiphosV2Partition(tableName, i, i * 5 * batchSize, (i + 1) * 5 * batchSize, batchSize)
    }
    parts
  }

  override def createReaderFactory(): PartitionReaderFactory = new XiphosV2PartitionReaderFactory()
}

class XiphosV2Partition(val tableName : String, val partitionIndex: Int, val start:Int, val end: Int, val batchSize : Int) extends
  FilePartition(partitionIndex, Array[PartitionedFile](PartitionedFile(null, tableName, 0, 0)))

class XiphosV2PartitionReaderFactory extends PartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = ???

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    new XiphosV2ColumunarPartitionReader(partition.asInstanceOf[XiphosV2Partition])
  }
}

class XiphosV2ColumunarPartitionReader(partition : XiphosV2Partition) extends PartitionReader[ColumnarBatch] {
  val tableName = partition.tableName
  private val maxItemsPerBatch = partition.batchSize
  var index = partition.start

  var columnsArray = Array[OnHeapColumnVector]()
  if (tableName == "test_table_1" || tableName == "test_table_2") {
    var id_column = new OnHeapColumnVector(maxItemsPerBatch, IntegerType)
    columnsArray = columnsArray :+ id_column
    var value_column = new OnHeapColumnVector(maxItemsPerBatch, StringType)
    columnsArray = columnsArray :+ value_column
    // add additional integer column for test_table_2
    if (tableName == "test_table_2") {
      var column2 = new OnHeapColumnVector(maxItemsPerBatch, IntegerType)
      columnsArray = columnsArray :+ column2
    }
  }
  var columnarBatch = new ColumnarBatch(columnsArray.toArray)

  override def next(): Boolean = index < partition.end

  override def get(): ColumnarBatch = {
    var n_items = min(partition.end - index, maxItemsPerBatch)
    columnarBatch.setNumRows(n_items)

    for (i <- 0 until n_items) {
      columnsArray(0).putInt(i, index + i)
      val stringValue = tableName + " " + (index + i).toString
      columnsArray(1).putByteArray(i, stringValue.getBytes(java.nio.charset.StandardCharsets.UTF_8))
      if (tableName == "test_table_2") {
        columnsArray(2).putInt(i, index + i)
      }
    }
    index += n_items
    columnarBatch
  }

  override def close(): Unit = Unit
}


