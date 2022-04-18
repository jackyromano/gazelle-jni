
spark.sparkContext.setLogLevel("INFO")
val root = sys.env.get("WORKSPACE").get
val data_dir = root + "/xiphos-spark-integration/resources"
println("PWD: " + sys.env.get("PWD"))
val table_df = spark.read.parquet(data_dir + "/test_strings.parquet")
table_df.createTempView("test_table")
table_df.printSchema
val query_df = spark.sql("select * from test_table")
query_df.explain
query_df.show(1)
:quit


