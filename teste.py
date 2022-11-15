from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.getOrCreate()

data1 = [("James","","Smith",10,"M",3000),
    ("Michael","Rose","",20,"M",4000),
    ("Robert","","Williams",30,"M",4000),
    ("Maria","Anne","Jones",40,"F",4000),
    ("Jen","Mary","Brown",50,"F",-1)
  ]

schema1 = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", IntegerType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])

data2 = [(10,"smith@gmail.com"),
    (30,"robert@gmail.com")
  ]

schema2 = StructType([ \
    StructField("id", IntegerType(), True), \
    StructField("email", StringType(), True)
  ])


df1 = spark.createDataFrame(data=data1,schema=schema1)
df2 = spark.createDataFrame(data=data2,schema=schema2)

df1.show(truncate=False)
df2.show(truncate=False)
