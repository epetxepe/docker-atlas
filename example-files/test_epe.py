import pyspark


spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.1") \
    .getOrCreate()

from delta.tables import *


data = spark.range(0, 5)
data.write.format("delta").save("/tmp/delta-table6")
