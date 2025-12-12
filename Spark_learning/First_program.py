from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Spark programs").getOrCreate()
print(spark.conf.get('spark.sql.shuffle.partitions'))
print(spark)
print("closing")
spark.stop()