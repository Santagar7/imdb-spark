import pyspark.sql.types as t

title_crew_schema = t.StructType(fields=[
    t.StructField(name='tconst', dataType=t.StringType(), nullable=False),
    t.StructField(name='directors', dataType=t.StringType(), nullable=False),
    t.StructField(name='writers', dataType=t.StringType(), nullable=True),
])