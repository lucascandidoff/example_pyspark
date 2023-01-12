from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DataType
from pyspark.sql.functions import col, lit, split, udf, when

spark = SparkSession.builder.getOrCreate()

data = [
      (1,10,"MATRIZ",10),
      (2,10,"FILIAL",0),
      (3,10,"FILIAL",0),
      (4,20,"MATRIZ",2),
      (5,20,"FILIAL",0),
      (6,20,"FILIAL",0)
    ]

schema = StructType([ \
    StructField("id", IntegerType(), True), \
    StructField("cod", IntegerType(), True), \
    StructField("tipo",StringType(),True), \
    StructField("qtd", IntegerType(), True)
  ])


df = spark.createDataFrame(data=data,schema=schema)  

# for i in df:
#     a=1

# df_temp = df.filter("tipo = 'MATRIZ'")

df.show()      

# dataCollect = df_temp.collect()
for row in df.filter("tipo = 'MATRIZ'").collect():
    df = df.withColumn('qtd', 
                    when( (df.cod == row['cod']) & (df.tipo == 'FILIAL'),row['qtd']) \
                   .otherwise(df.qtd))

df.show()      

# def atualiza_total_contatos_filiais(logradouro):  
#   for i in lista_tipo_logradouro:
#     if i in logradouro:      
#       return i.strip()
#   return ''

# atualiza_total_contatos_filiais_UDF = udf(atualiza_total_contatos_filiais,StringType())    

# df = df.withColumn('qtd', atualiza_total_contatos_filiais_UDF(col('cod')))


# df.show()