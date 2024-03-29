from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DataType
from pyspark.sql.functions import col, lit, split

GLOBAL_VARIABLE = 'teste'

teste = ''

def teste1(x):
  local_variable = x
  y=x

def teste2(x):
  variable_teste = x
  w=x


def teste3(n):
  n2 = 0
  for i in range(n):
    n2 = n2 + i


teste1('Text')
teste3(10)

spark = SparkSession.builder.getOrCreate()

data1 = [("James,teste","","Smith",10,"M",3000),
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

data2 = [(10,"Teste1","smith@gmail.com"),
      (30,"Teste2","robert@gmail.com")
  ]

schema2 = StructType([ \
    StructField("id", IntegerType(), True), \
    StructField("firstname",StringType(),True), \
    StructField("email", StringType(), True)
  ])


df1 = spark.createDataFrame(data=data1,schema=schema1)
df2 = spark.createDataFrame(data=data2,schema=schema2)

df_teste1 = df1

for i in df1.columns:
  if i == 'id': continue
  df_teste1 = df_teste1.withColumnRenamed(i,f"{i}_teste1")

for i in df2.columns:
  if i == 'id': continue
  df_teste2 = df_teste2.withColumnRenamed(i,f"{i}_teste2")  

df_teste1.show()



# df = df1.join(df_municipio, on=['id_localidade'], how='left')                    

# df = df1.merge(df2, left_on='lkey', right_on='rkey')

# df = df1.join(df2, on=['id'], how='left')

# df = df1.join(df2, df1.id == df2.id, "left")    

# df2 = df2.withColumn('column_teste', lit('teste'))

# df = df.select(col('id'),col('email'))

# df1 = df1.withColumn('year', split(df1['firstname'], ',').getItem(1))

# df.show(truncate=False)
