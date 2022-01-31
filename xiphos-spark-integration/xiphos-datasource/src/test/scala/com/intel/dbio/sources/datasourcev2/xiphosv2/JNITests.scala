package com.intel.dbio.sources.datasourcev2.xiphosv2

import org.junit.Assert.assertTrue
import org.junit.Test
import org.scalatest.Ignore

@Ignore
class JNITests {

  @Test
  def JNIInit = {
    assertTrue(XiphosJNI.get.init("hello there"))
  }
  @Test
  def JNIGetTableDesc = {
    val tables : Array[String] = Array("test_table_1", "test_table_2")
    tables.foreach( t => {
      println(XiphosJNI.get.getSchemaDesc(t))
    })
  }
}
