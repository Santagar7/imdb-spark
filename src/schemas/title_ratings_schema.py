import pyspark.sql.types as t

title_ratings_schema = t.StructType(fields=[
    t.StructField(name='tconst', dataType=t.StringType(), nullable=False),
    t.StructField(name='averageRating', dataType=t.FloatType(), nullable=True),
    t.StructField(name='numVotes', dataType=t.IntegerType(), nullable=True)
])