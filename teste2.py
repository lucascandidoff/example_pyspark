from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, lit, split, to_date
import datetime

spark = SparkSession.builder.getOrCreate()

data1 = [(10,"Lucas"),
      (30,'Bernardo')
  ]

schema1 = StructType([ \
    StructField("id", IntegerType(), True), \
    StructField("nome",StringType(),True)
  ])

data2 = [(10, datetime.datetime.strptime('2022-01-01', "%Y-%m-%d").date()),
      (30, datetime.datetime.strptime('2022-06-01', "%Y-%m-%d").date())
  ]

schema2 = StructType([ \
    StructField("id", IntegerType(), True), \
    StructField("date_teste",DateType(),True)
  ])

df1 = spark.createDataFrame(data=data1,schema=schema1)
df2 = spark.createDataFrame(data=data2,schema=schema2)

df = df1.join(df2, on=['id'], how='left')

df.show()
