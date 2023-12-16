import pyspark.sql.types as t

title_principals_schema = t.StructType(fields=[
    t.StructField(name='tconst', dataType=t.StringType(), nullable=False),
    t.StructField(name='ordering', dataType=t.IntegerType(), nullable=False),
    t.StructField(name='nconst', dataType=t.StringType(), nullable=False),
    t.StructField(name='category', dataType=t.StringType(), nullable=False),
    t.StructField(name='job', dataType=t.StringType(), nullable=True),
    t.StructField(name='characters', dataType=t.StringType(), nullable=True),
])