package com.intel.dbio.sources.datasourcev2.xiphosv2

import org.junit.Assert.{assertEquals, assertNotNull, assertTrue}
import org.junit.{Ignore, Test}

class JNITests {
  @Ignore
  @Test
  def JNIGetTableDesc = {
    val schema =XiphosJNI.getSchema("lineitem_quantity")
    assertNotNull(schema)
    assertEquals(16,  schema.fields.length)
  }
}
