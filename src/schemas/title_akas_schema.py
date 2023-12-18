import pyspark.sql.types as t

title_akas_schema = t.StructType(fields=[
    t.StructField(name='titleId', dataType=t.StringType(), nullable=False),
    t.StructField(name='ordering', dataType=t.IntegerType(), nullable=False),
    t.StructField(name='title', dataType=t.StringType(), nullable=False),
    t.StructField(name='region', dataType=t.StringType(), nullable=True),
    t.StructField(name='language', dataType=t.StringType(), nullable=True),
    t.StructField(name='types', dataType=t.StringType(), nullable=True),
    t.StructField(name='attributes', dataType=t.StringType(), nullable=True),
    t.StructField(name='isOriginalTitle', dataType=t.IntegerType(), nullable=False)
])