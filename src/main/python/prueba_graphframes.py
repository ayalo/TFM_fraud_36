from typing import Optional, Any

from pyspark import *
from pyspark.sql import *
import pandas as pd
from graphframes import *

from pyspark.sql import functions as F
from pyspark.sql import *
from pyspark.sql import SparkSession
import pandas as pd
from graphframes import *



def main():
    '''Program entry point'''



    # Intialize a spark context
    with F.SparkContext( "local", "PySparCreateDataframe" ) as spark:
        data = pd.DataFrame(
            {'domain': ['example.org',
                        'site.com',
                        'example.org',
                        'example.org',
                        'website.com',
                        'website.com',
                        'website.com',
                        'example.org',
                        'example.org',
                        'example.org'],
             'IP': ['10.20.30.40',
                    '30.50.70.90',
                    '10.20.30.41',
                    '10.20.30.42',
                    '90.80.70.10',
                    '30.50.70.90',
                    '30.50.70.90',
                    '10.20.30.42',
                    '10.20.30.40',
                    '10.20.30.40']})
             #'subdomain': ['test1', 'something', 'test2', 'test3', 'else', 'else', 'else', 'else', 'else', 'else']} )
        sqlContext = SQLContext( spark )
        df = sqlContext.createDataFrame( data )
        print (" Pintamos Dataframe completo :")
        df.show()

        df_dom= df.select("domain")
        df_ip=df.select("IP")
        df_vertices=df_dom.union(df_ip)
        #print (" Pintamos Dataframe vertices despues union:") # si metemos mas valores que D-IP
        #df_vertices.show()
        #df_vertices = df.groupBy( "domain", "IP" )
        print (" Pintamos Dataframe vertices :")
        df_vertices.show()

        df_count_domain_ips = df.select("domain","IP").groupBy("domain").agg(F.collect_list(F.col("IP")).alias("IP_list"))
        print( " Pintamos DF df_count_domain_ips.show :" )
        df_count_domain_ips.show()

        rdd_count_domain_ips = df_count_domain_ips.rdd.map(lambda x:(x.domain,x.IP_list,len(x.IP_list)))
        df_count_domain_ips=rdd_count_domain_ips.toDF(["domain", "IP_list", "total_links"])
        print( " Pintamos DF df_count_domain_ips.show :" )
        df_count_domain_ips.show()

        df_edges=df.groupBy("domain", "IP").count()
        df_edges.show()
        #df1['Value'] = df1['Type'].map( df2.set_index( 'Type' )['Value'] )

        #df_edges['domain']=rdd_count_domain_ips[]
        print("despues de df_edges")

        #g = GraphFrame(df, df_edges)

        #print("Pasa de GrapFrame")
        #g.df.show()
        #g.df_edges.show()





if __name__ == "__main__":
    main()