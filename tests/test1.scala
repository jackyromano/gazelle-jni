
spark.sparkContext.setLogLevel("INFO")
val root = sys.env.get("WORKSPACE").get
val tpchData = root + "/jvm/src/test/resources/tpch-data"
println("PWD: " + sys.env.get("PWD"))
val call_center = spark.read.parquet(tpchData + "/customer")
call_center.createTempView("customer")
println("customer_schema:")
call_center.printSchema
val query_df = spark.sql("select c_name, c_custkey from customer")
query_df.explain
query_df.show
:quit


