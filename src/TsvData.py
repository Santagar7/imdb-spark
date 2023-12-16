from pyspark.sql import SparkSession
from pyspark.sql import types

class TsvData:
    def __init__(self, spark_session: SparkSession, schema: types.StructType, path: str):
        self.data = spark_session.read.csv(path, sep='\t', nullValue='\\N', header=True, schema=schema)

    def head(self):
        return self.data.show(n=10)