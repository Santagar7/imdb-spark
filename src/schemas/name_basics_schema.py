import pyspark.sql.types as t

name_basics_schema = t.StructType(fields=[
    t.StructField(name='nconst', dataType=t.StringType(), nullable=False),
    t.StructField(name='primaryName', dataType=t.StringType(), nullable=False),
    t.StructField(name='birthYear', dataType=t.IntegerType(), nullable=True),
    t.StructField(name='deathYear', dataType=t.IntegerType(), nullable=True),
    t.StructField(name='primaryProfession', dataType=t.StringType(), nullable=False),
    t.StructField(name='knownForTitles', dataType=t.StringType(), nullable=False)
])