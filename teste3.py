from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *


# def sep_num(texto,delimitador,posicao):
#   lista = []
#   for i in texto:
#     lista.append('')

#   if ',' in endereco:
#     end = endereco.split(',')[0].strip()
#     num = endereco.split(',')[1].strip()
  
#   if campo == 'logradouro':
#     return end
#   if campo == 'numero':
#     return num

# sep_num_UDF = udf(sep_num,StringType())    

# df = df1.withColumn('logradouro', sep_num_UDF(col('endereco'),lit('logradouro')))
# df = df.withColumn('numero', sep_num_UDF(col('endereco'),lit('numero')))

lista_tipo_logradouro = [
    'AEROPORTO',
    'ALAMEDA',
    'AREA',
    'AVENIDA',
    'CAMPO',
    'CHACARA',
    'COLONIA',
    'CONDOMINIO',
    'CONJUNTO',
    'DISTRITO',
    'ESPLANADA',
    'ESTACAO',
    'ESTRADA',
    'FAVELA',
    'FAZENDA',
    'FEIRA',
    'JARDIM',
    'LADEIRA',
    'LAGO',
    'LAGOA',
    'LARGO',
    'LOTEAMENTO',
    'MORRO',
    'NUCLEO',
    'PARQUE',
    'PASSARELA',
    'PATIO',
    'PRACA',
    'QUADRA',
    'RECANTO',
    'RESIDENCIAL',
    'RODOVIA',
    'RUA',
    'SETOR',
    'SITIO',
    'TRAVESSA',
    'TRECHO',
    'TREVO',
    'VALE',
    'VEREDA',
    'VIA',
    'VIADUTO',
    'VIELA',
    'VILA'
    'AV '
    'PRC '
]


spark = SparkSession.builder.getOrCreate()

data1 = [(10,"AV SEGISMUNDO"),
         (20,"RUA FLORIANO"),
         (30,"R FLORIANO"),
         (30,"FLORIANO"),
      ]

schema1 = StructType([ \
    StructField("cod",IntegerType(),True), \
    StructField("endereco",StringType(),True)
  ])


df1 = spark.createDataFrame(data=data1,schema=schema1)

def separa_tipo_logradouro(logradouro):  
  for i in lista_tipo_logradouro:
    if i in logradouro:      
      return i.strip()
  return ''

def separa_logradouro(logradouro):  
  for i in lista_tipo_logradouro:
    if i in logradouro:      
      return logradouro.replace(i,'').strip()
  return logradouro
      
 
separa_tipo_logradouro_UDF = udf(separa_tipo_logradouro,StringType())    
separa_logradouro_UDF = udf(separa_logradouro,StringType())    

df = df1.withColumn('tipo', separa_tipo_logradouro_UDF(col('endereco')))
df = df.withColumn('logradouro', separa_logradouro_UDF(col('endereco')))

# df = df.withColumn('tipo', expr('list_logradouro')[0])


df.show()