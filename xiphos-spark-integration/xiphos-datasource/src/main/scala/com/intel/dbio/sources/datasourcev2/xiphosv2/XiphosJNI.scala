/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.dbio.sources.datasourcev2.xiphosv2

import org.apache.spark.sql.types.{DataType, DateType, IntegerType, StringType, StructType}

object XiphosJNI {
  private var instance : XiphosJniImp = null
  def get : XiphosJniImp = {
    if (instance == null) {
      instance = new XiphosJniImp
    }
    instance
  }
  def init: Boolean = {
    get.init("just a test")
  }
  def getSchema(tableName : String) : StructType = {
    var st = new StructType
    val desc = get.getSchemaDesc(tableName)
    desc.split(" +").foreach(f => {
      val field_desc = f.split(":")
      st = st.add(
        field_desc(0),
        getDataType(field_desc(1)),
        isNullable(field_desc(2)))
    })
    //scalastyle:off
    println(st)
    // scalastyle:on
    st
  }
  def getDataType(s : String): DataType = {
    s match {
      case "s" => StringType
      case "i" => IntegerType
      case "d" => DateType
      case _ => {
        throw new RuntimeException("Unsupported data type")
      }
    }
  }
  def isNullable(s : String) : Boolean = {
    s match {
      case "false" => false
      case _ => true
    }
  }
}
