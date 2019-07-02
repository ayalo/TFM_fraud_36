from src.main.python.IpCleaner import ip_cleaner
from src.main.python.DomainCleaner import domain_cleaner

from pyspark.sql.functions import udf
from pyspark.sql.types import *

from pyspark.sql import SparkSession
import pandas as pd


def main():
    '''Program entry point'''
    # Intialize a spark context
    # with F.SparkContext( "local", "PySparCreateDataframe" ) as spark:
    data = pd.DataFrame(
        {'referrer_domain': ['example.org',
                    'site.com',
                    'example.org',
                    'example\x00\x11.org',
                    'website.com',
                    'wwww.website.com/otromas/kaka',
                    'website.com',
                    'example.org',
                    'example.org',
                    'http://example.org',
                    'website.com',
                    'website.com',
                    'example.org'],
         'user_ip': ['10.20.30.40',
                '30.50.70.90',
                '10.20.30.41',
                '10.20.30.42',
                '90.80.70',
                '30.50.70.90',
                '30.50.70.90',
                '10.20.30.42',
                '10.20.30.40',
                '10.20.30.40.40',
                '10.20.30.40',
                '10.20.30.42',
                '30.50.70.90']} )
    # 'subdomain': ['test1', 'something', 'test2', 'test3', 'else', 'else', 'else', 'else', 'else', 'else']} )
    # sqlContext = SQLContext( spark )
    spark = SparkSession.builder.getOrCreate()
    df= spark.createDataFrame(data)
    print( " Pintamos Dataframe completo :" )
    df.show()

    # Filtramos los null para que no den problemas (ojo con los datos reales poner esto :
    # df_current= df.filter (" referrer_domain is not null and ssp_domain is not null and user_ip is not null")
    df_current = df.filter( " referrer_domain is not null and user_ip is not null" )


    # Añado una columna mas al df_current con el user_ip normalizado : ip_cleaned
    # Añado una columna mas al df_current con el referrer_domain normalizado : domain_cleaned

    udf_ipcleaner = udf(ip_cleaner, StringType())
    udf_domaincleaner = udf(domain_cleaner, StringType())

    df_cleaned_ip = df_current.withColumn('ip_cleaned', udf_ipcleaner(df_current.user_ip))
    df_cleaned_dom = df_cleaned_ip.withColumn('domain_cleaned', udf_domaincleaner(df_current.referrer_domain))
    df_cleaned_dropna = df_cleaned_dom.dropna(subset=('user_ip','referrer_domain','ip_cleaned','domain_cleaned'))

    df_cleaned  = df_cleaned_dropna
    df_cleaned.printSchema()
    ##df_cleaned.limit(5).toPandas()
    df_cleaned.show() ## esta linea da error en el cluster



if __name__ == "__main__":
    main()