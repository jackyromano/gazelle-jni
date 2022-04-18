//spark.sparkContext.setLogLevel("DEBUG")
val strings_df = spark.read.format("com.intel.dbio.sources.datasourcev2.xiphosv2").load("strs")
strings_df.createTempView("test_strings")
val query_df = spark.sql("select * from test_strings")
query_df.explain
query_df.show
:quit


