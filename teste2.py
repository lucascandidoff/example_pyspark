from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, regexp_replace, when, split, concat, substring, length


spark = SparkSession.builder.getOrCreate()

data = [('lucas',"34996843222",'MG'),
    ('Ber',"3232384410",'MG'),
    ('Manu',"1120593247",None)
  ]

schema = StructType([ \
    StructField("nome", StringType(), True), \
    StructField("telefone", StringType(), True), \
    StructField("uf", StringType(), True)        
  ])


df_result = spark.createDataFrame(data=data,schema=schema)


def trata_telefone_br(df, col_telefone, col_ddd, col_uf):

    # RETIRA +55
    df = df.withColumn(col_telefone, regexp_replace(col(col_telefone), "\+55", ''))
    # DEIXA SOMENTE NUMEROS
    df = df.withColumn(col_telefone, regexp_replace(col_telefone, "[^0-9]",""))
    # PRECISA TER 10 OU 11 CARACTERES PARA SER UM NUMERO FORMAL (DDD + NUMERO FIXO OU MOVEL)
    df = df.withColumn(col_telefone, when(~length(col(col_telefone)).isin(10, 11), None).otherwise(col(col_telefone)))
    # LISTA DOS DDDs DO BRASIL https://totalip.com.br/quais-os-ddds-de-cada-estado-do-brasil/
    df = df.withColumn(col_telefone, 
        when(~substring(col(col_telefone), 0, 2).isin 
            ('61', '62', '64', '65', '66', '67', '82', '71', '73', '74', '75', '77', '85', '88', '98', '99', '83', '81', '87', '86', '89', '84', '79', 
             '68', '96', '92', '97', '91', '93', '94', '69', '95', '63', '27', '28', '31', '32', '33', '34', '35', '37', '38', '21', '22', '24', '11', 
             '12', '13', '14', '15', '16', '17', '18', '19', '41', '42', '43', '44', '45', '46', '51', '53', '54', '55', '47', '48', '49'), None)\
        .otherwise(col(col_telefone))
    )
    # PEGAR O DDD
    df = df.withColumn(col_ddd, substring(col(col_telefone), 0, 2))
    
    # RETIRAR O DDD DO NUMERO
    df = df.withColumn(col_telefone, substring(col(col_telefone), 3, 9))
    
    # ADICIONAR O ZERO A ESQUERDA DO DDD
    df = df.withColumn(col_ddd, concat(lit('0'), df.ddd))
    # SE O NUMERO TIVER 8 CARACTERES E COMEÇAR POR 6, 7, 8 OU 9, ENTAO É MOVEL E PRECISA ACRESCENTAR O 9 
    # 2 a 5 são números fixos (linha terrestre) 6 a 9 são números móveis (telemóvel) https://pt.stackoverflow.com/questions/14343/como-diferenciar-tipos-de-telefone
    df = df.withColumn(col_telefone, 
        when(
            (length(col(col_telefone)) == 8) & 
            (substring(col(col_telefone), 1, 1).isin('6', '7', '8', '9')), concat(lit('9'), col(col_telefone))
        ).otherwise(col(col_telefone))
    )
    
    df = df.withColumn(col_telefone, regexp_replace(col_telefone, '\(|\)|-', ''))
    df = df.withColumn(col_telefone, when(col(col_telefone).rlike('^\d{2} \d{8}$'), split(df[col_telefone],' ').getItem(1))\
                        .when(col(col_telefone).rlike('^\d{2} \d{9}$'), split(df[col_telefone],' ').getItem(1))\
                        .when(col(col_telefone).rlike('^\d{9}$'), col(col_telefone))\
                        .when((col(col_telefone).rlike('^\d{8}$')) & (col(col_telefone).startswith('5')) & (col(col_uf) == 'SP'), concat(lit('9'),col_telefone)) \
                        .when(col(col_telefone).rlike('^(6|7|8|9)\d{7}'), concat(lit('9'),col_telefone))\
                        .when(col(col_telefone).rlike('^\d{10}$'), split(df[col_telefone],' ').getItem(1))\
                        .when(col(col_telefone).rlike('^(0800|0500|0505|0300|0900|4004|3003|01803)'), col(col_telefone))
                        .when(col(col_telefone).startswith('+'), '')\
                        .when(col(col_telefone).rlike('[a-zA-Z ]+'), regexp_replace(col_telefone,'(.+)',''))\
                        .otherwise(col(col_telefone)))

    # if col_uf:
    #     df = df.withColumn(col_telefone, \
    #         when((col(col_telefone).rlike('^\d{8}$')) & (col(col_telefone).startswith('5')) & (col(col_uf) == 'SP'), concat(lit('9'),col_telefone)) \
    #         .otherwise(col(col_telefone)))

    
    return df

df_result.show()

df_result = trata_telefone_br(df_result, 'telefone','ddd', 'uf')

df_result.show()


                          
# df = df.withColumn('ddd',  when(col('telefone').rlike('^\d{2} \d{8}$'), split(df['telefone'],' ').getItem(0))\
#                    .when(col('telefone').rlike('^\d{2} \d{9}$'), split(df['telefone'],' ').getItem(0))\
#                    .when(col('telefone').isNull(), None)\
#                    .otherwise(col('ddd')))


