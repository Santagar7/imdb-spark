from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

spark_session = (SparkSession.builder
                 .master("local")
                 .appName("task app")
                 .config(conf=SparkConf())
                 .getOrCreate())


data = [("Tonya", 18), ("Nina", 41)]
schema = t.StructType([
    t.StructField("name", t.StringType(), True),
    t.StructField("age", t.IntegerType(), True)
])
df = spark_session.createDataFrame(data, schema)
df.show()

df.printSchema()

df.explain(mode="extended")



