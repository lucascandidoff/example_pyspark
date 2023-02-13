from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DataType, ArrayType
from pyspark.sql.functions import col, lit, split, udf, when, explode_outer, collect_list, first, row_number, monotonically_increasing_id

spark = SparkSession.builder.getOrCreate()

# data = [
#       (1,10,"MATRIZ",10),
#       (2,10,"FILIAL",0),
#       (3,10,"FILIAL",0),
#       (4,20,"MATRIZ",2),
#       (5,20,"FILIAL",0),
#       (6,20,"FILIAL",0)
#     ]

# schema = StructType([ \
#     StructField("id", IntegerType(), True), \
#     StructField("cod", IntegerType(), True), \
#     StructField("tipo",StringType(),True), \
#     StructField("qtd", IntegerType(), True)
#   ])


data = [
      ('Lucas',['teste1','teste2','teste3']),
      ('Matheus',['teste4','teste5','teste6']),
      ('Thais',['aaaaa']),
      ('Ber',[None])
    ]

schema = StructType([ \
    StructField("nome", StringType(), True), \
    StructField("array_field", ArrayType(StringType()), True) \
  ])     

df = spark.createDataFrame(data=data,schema=schema)  

df = df.withColumn('link', explode_outer('array_field'))
df.show()

# window = Window.orderBy("nome")

# Adicionando uma nova coluna para numerar os registros
# df = df.withColumn("index", row_number().over(window))

# for j in range(df.count()):
#   for i, item in enumerate(df.select("array_field").filter(f"index == {j+1}").first()[0]):
#       if not f"item{i}" in df.columns:
#         df = df.withColumn(f"item{i}", when(df['index'] == j+1, lit(item)))
#       else:
#         df = df.withColumn(f"item{i}", when(df['index'] == j+1, lit(item)).otherwise(col(f"item{i}")))

# Visualize o resultado

# udf1 = udf(lambda x : x.split()[0])
# df = df.select('index', explode('array_field').alias('link'), udf1(col('link')).alias('indextype'))

# df.show()

# df = df.groupby('index').pivot('indextype').agg(first('link')).join(df,'index','left')

# df.show()



# for i in df:
#     a=1

# df_temp = df.filter("tipo = 'MATRIZ'")

# df.show()      

# dataCollect = df_temp.collect()

# def separete_links(links):
#   n=0
#   for link in links:
#     n=n+1
#     df = df.withColumn(f'link_{n}', lit(i)) 
    



exit()

df_pd = df.toPandas()

# print(df_pd)

for i in range(len(df_pd)):
  n=0
  for link in df_pd['links'][i]:    
    n=n+1
    if not f'link_{n}' in df_pd:
      df_pd[f'link_{n}'] = ''
    df_pd[f'link_{n}'][i] = link

df=spark.createDataFrame(df_pd) 

df.show()
# def atualiza_total_contatos_filiais(logradouro):  
#   for i in lista_tipo_logradouro:
#     if i in logradouro:      
#       return i.strip()
#   return ''

# atualiza_total_contatos_filiais_UDF = udf(atualiza_total_contatos_filiais,StringType())    

# df = df.withColumn('qtd', atualiza_total_contatos_filiais_UDF(col('cod')))


# df.show()