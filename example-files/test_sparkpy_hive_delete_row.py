from os.path import abspath

from pyspark.sql import SparkSession
from pyspark.sql import Row

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example - 2") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# spark is an existing SparkSession
#spark.sql("CREATE TABLE IF NOT EXISTS src2 (key INT, value STRING) USING hive")
spark.sql("DELETE FROM src2 WHERE key = 238")

# Queries are expressed in HiveQL
spark.sql("SELECT * FROM src2").show()
# +---+-------+
# |key|  value|
# +---+-------+
# |238|val_238|
# | 86| val_86|
# |311|val_311|
# ...

# Aggregation queries are also supported.
spark.sql("SELECT COUNT(*) FROM src2").show()
# +--------+
# |count(1)|
# +--------+
# |    500 |
# +--------+
