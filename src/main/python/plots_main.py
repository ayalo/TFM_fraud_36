import pandas as pd

import matplotlib.pyplot as plt

from utils.gf_utils import *
from utils.df_utils import *
from utils.draw_utils import *
from utils.spark_utils import *
from utils.read_write_utils import *
import numpy as np



def main():
    '''Program entry point

    :return gf : graph graphframe Domain-IP bipartite relation
    '''

    data = pd.DataFrame(
        {'referrer_domain': ['example.org',
                             'site.com',
                             'example.org',
                             'example.org',
                             'website.com',
                             'website.com',
                             'website.com',
                             'example.org',
                             'example.org',
                             'example.org',
                             'website.com',
                             'website.com',
                             'example.org'],
         'user_ip': ['10.20.30.40',
                     '30.50.70.90',
                     '10.20.30.41',
                     '10.20.30.42',
                     '90.80.70.10',
                     '30.50.70.90',
                     '30.50.70.90',
                     '10.20.30.42',
                     '10.20.30.40',
                     '10.20.30.40',
                     '10.20.30.40',
                     '10.20.30.42',
                     '30.50.70.90']} )
    # 'subdomain': ['test1', 'something', 'test2', 'test3', 'else', 'else', 'else', 'else', 'else', 'else']} )


    # Intialize a spark context
    spark = spark_session()
    df = spark.createDataFrame( data )

    '''
    # SAVING gf_dom_ip graph locally
    df = spark.read.format( "csv" ).option( "header", 'true' ).option( "delimiter", ',' ).load(
        "/Users/olaya/Documents/Master/TFM/Datos/180208/ssp_bid_compressed_000000000499.csv.gz" )
    df = clean( df, "referrer_domain", "user_ip" )

    g_domip = get_graph_domip( df, 15 ).persist()
    gf_write( g_dom_dom, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_ip" )

    '''
    '''
    # SAVING df_degree_ratio dataframe locally (need g_domip previously calculated
    g_domip = gf_read( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_ip" )
    df_degree_ratio=get_df_degree_ratio(g_domip)
    df_write(df_degree_ratio, /Users/olaya/Documents/Master/TFM/output_fraud/df_degree_ratio)
    '''
    '''
       # SAVING g_dom_dom graph locally
       g_domip = gf_read( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_ip" )
       g_domdom = get_graph_domdom( g_domip ).persist()
       gf_write( g_domdom, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_domain" )
    '''

    # LOAD LOCALLY DATA
    #g_domip = gf_read( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_ip" )
    #df_degree_ratio = df_read( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/df_degree_ratio" )
    #g_domdom=gf_read( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_domain" )



    print( "plots_main MAIN-- calculando doms_more_visited : show todo.." )
    sorted_degrees = most_visited( g_domip ).limit(4)
    sorted_degrees.printSchema()
    
    print( "plots_main MAIN-- prueba" )
    degree, id = zip(
        *[(item.degree, item.id) for item in sorted_degrees.select( "degree", "id.id" ).collect()] )

    ####--> Grafica : y= (edge_ratio = covisitation degree) x=dominios ó x=IPS (sacar las dos)

    fig, ax = plt.subplots()

    ax.bar(id,degree)
    ax.bar( id, degree )

    ax.set_xticks( id  )
    #ax.xlabel( "domain", fontsize=7)
    #ax.ylabel( "degree", fontsize=7 )
    #ax.title( "most visited domains", fontsize=7 )
    plt.show()
    ####-->FIN

    ####---> Grafica : y= (edge_ratio = covisitation degree) x=dominios ó x=IPS (sacar las dos)
    print( "plots_main MAIN-- calculando count collect de edge_ratio=y .." )
    edge_ratio, domain_cleaned = zip(
        *[(item.edge_ratio, item.id) for item in df_degree_ratio.select( "edge_ratio", "id" ).collect()] )
    ##print( f"edge {edge_ratio} " )
    ##print( f"domain_cleaned {domain_cleaned} " )
    # domain_cleaned = [item[0] for item in df_degree_ratio.groupBy( "id" ).count().collect()]
    # print( domain_cleaned.schema )
    print( "plots_main MAIN-- calculando dataframe x e y  .." )
    ratio_per_domain = {"edge_ratio": edge_ratio, "domain_cleaned": domain_cleaned}
    # print( ratio_per_domain.schema )
    #
    plt.plot( edge_ratio, domain_cleaned, 'ro' )

    '''
    #ratio_per_domain = df_degree_ratio.DataFrame( ratio_per_domain )
    ratio_per_domain = ratio_per_domain.toDF( ["edge_ratio", "domain_cleaned"] )
    ratio_per_domain.head()
    #%matplotlib inline

    print ("plots_main MAIN-- ratio_per_domain sort edge_ratio  ..")
    ratio_per_domain = ratio_per_domain.sort_values(by = "edge_ratio")
    print ("plots_main MAIN-- ratio_per_domain plot   ..")
    ratio_per_domain.plot(figsize = (20,10), kind = "bar", color = "red",
                                   x = "domain_cleaned", y = "edge_ratio", legend = False)
    '''
    plt.xlabel( "", fontsize=10 )
    plt.ylabel( "edge_ratio or covisitation_degree", fontsize=10 )
    plt.title( "domain_cleaned", fontsize=14 )
    plt.xticks( size=10 )
    plt.yticks( size=10 )

    print( "plots_main MAIN-- antes de plot.show   .." )
    plt.show()
    ####--->  FIN

'''
    y=df_degree_ratio.edge_ratio
    x1=df_degree_ratio.domain_cleaned
    x2=df_degree_ratio.ip_cleaned


    df_degree_ratio.plot(kind='bar',x='domain_cleaned',y='edge_ratio')
'''


if __name__ == "__main__":
    main()
