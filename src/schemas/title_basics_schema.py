import pyspark.sql.types as t

title_basics_schema = t.StructType(fields=[
    t.StructField(name='tconst', dataType=t.StringType(), nullable=False),
    t.StructField(name='titleType', dataType=t.StringType(), nullable=False),
    t.StructField(name='primaryTitle', dataType=t.StringType(), nullable=False),
    t.StructField(name='originalTitle', dataType=t.StringType(), nullable=False),
    t.StructField(name='isAdult', dataType=t.IntegerType(), nullable=False),
    t.StructField(name='startYear', dataType=t.IntegerType(), nullable=True),
    t.StructField(name='endYear', dataType=t.IntegerType(), nullable=True),
    t.StructField(name='runtimeMinutes', dataType=t.IntegerType(), nullable=True),
    t.StructField(name='genres', dataType=t.StringType(), nullable=True)
])