import sys
import re
import string
import pyspark
from pyspark.sql import functions as F
from pyspark.sql import *
from pyspark.sql import SparkSession
import pandas as pd
from graphframes import *



def main():
    '''Program entry point'''



    # Intialize a spark context
    with F.SparkContext( "local", "PySparCreateDataframe" ) as spark:
        #data = {'visitor': ['sig', 'bar', 'jelmer'],
        #        'A': [1, 1, 0],
        #        'B': [1, 0, 1],
        #        'C': [-1, 0, 0]}
        sqlContext = SQLContext( spark )
        #df = pd.DataFrame( data )
        #df_test = sqlContext.createDataFrame( df )
        #df_test.show()

        data = [["174.57.221.111", "None", "Apache - HttpAsyncClient / 4.1.1", "bet.com", "bet.com", "2018 - 02 - 08 01:33: 48"],
                ["92.14.237.170", "3iqK4kIMgceeg + aAF0Rzu3onCmQ =2", "be2 / 1.0", "ebay.co.uk", "ebay.co.uk", "2018 - 02 - 08 01: 15:26"],
                ["183.14.52.248", "None", "be2 / 1.02", "ebay.com", "ebay.com", "2018 - 02 - 08 01:36: 36"],
                ["27.97.1.251", "2G6ngdnlVF20krDAvsb0XMSNXO2c =2", "be2 / 1.0", "apps.facebook.com", "apps.facebook.com", "2018 - 02 - 08 01: 43:18"],
                ["24.9.191.223", "FaKVFgh6cf2yXZvCiQed4hCL4=", "be2/1.0", "na.op.gg,op.gg", "2018-02-08 01:30:07"],
                ["10.64.35.574", "None", "None", "None", "None", "2018-02-08 01:30:07"],
                ["10.64.35.575", "None", "UnoNoNull", "None", "None", "2018-02-08 01:30:07"],
                ["10.64.35.576", "DosNoNull", "None", "None", "None", "2018-02-08 01:30:07"],
                ["10.64.35.577", "TresNoNull", "TresNoNull", "None", "None", "2018-02-08 01:30:07"],
                ["10.64.35.578", "TresNoNull", "TresNoNull", "CuatroNoNull", "None", "2018-02-08 01:30:07"],
                ["10.64.35.579", "TresNoNull", "TresNoNull", "None", "CincoNoNull", "2018-02-08 01:30:07"],
                ["10.64.35.580", "None", "None", "CuatroNoNull", "CincoNoNull", "2018-02-08 01:30:07"],
                ["174.57.221.111”, ”None", "Apache-HttpAsyncClient/4.1.1", "bet.com", "bet.com", "2018-02-08 01:33:48"]]

        df = pd.DataFrame( data, columns= ['user_ip', 'uuid_hashed', 'useragent', 'referrer_domain', 'ssp_domain', 'date_time'])
        df_text = sqlContext.createDataFrame( df )
        print (" Pintamos Dataframe completo :")
        df_text.show()
        #df_text.head(3)
        # vertices :
        df_count_domain_ips = df_text.select("referrer_domain","user_ip").groupBy("referrer_domain").agg(F.collect_list(F.col("user_ip")).alias("IP_list"))
        print (" Tabla Dominio - Ips (que han visitado el dominio)  :")
        df_count_domain_ips.show()
        print (" DF Dominio - Ips - links :")
        rdd_count_domain_ips = df_count_domain_ips.rdd.map(lambda x:(x.referrer_domain,x.IP_list,len(x.IP_list)))
        print ("RDD object:")
        rdd_count_domain_ips.take(13)
        df_count_domain_ips = rdd_count_domain_ips.toDF( ["referrer_domain", "IP_list", "total_visited_ips"] )
        print( " Pintamos DF df_count_domain_ips.show :" )
        df_count_domain_ips.show()
        #df_count_domain_ips['interCounter']=[len(set(a) & set (b)) for a,b in df_count_domain_ips.select('domain')


if __name__ == "__main__":
    main()
