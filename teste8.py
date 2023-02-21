from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DataType, ArrayType
from pyspark.sql.functions import col, lit, split, udf, when, explode_outer, collect_list, first, row_number, monotonically_increasing_id

spark = SparkSession.builder.getOrCreate()

# Criação de exemplo dos dois dataframes
df1 = spark.createDataFrame([("John", 25), ("Jane", 30), ("Jim", 35)], ["name", "age"])
df2 = spark.createDataFrame([("Jane", 'teste1'), ("Jim", 'll'), ("Joan", '112121')], ["name", "dominio"])

# Utilizando o método except para encontrar nomes exclusivos em df1
result = df1.select("name").subtract(df2.select("name"))

df = df1.join(result, on=['name'], how='inner')
df.show()


