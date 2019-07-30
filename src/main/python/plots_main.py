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
    #df = spark.createDataFrame( data )

    '''
    # SAVING gf_dom_ip graph locally
    df = spark.read.format( "csv" ).option( "header", 'true' ).option( "delimiter", ',' ).load(
        "/Users/olaya/Documents/Master/TFM/Datos/180208/ssp_bid_compressed_000000000499.csv.gz" )
    df = clean( df, "referrer_domain", "user_ip" )

    g_domip = get_graph_domip( df, 15 ).persist()
    gf_write_parquet( g_dom_dom, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_ip" )

    '''
    '''
    # SAVING df_degree_ratio dataframe locally (need g_domip previously calculated
    g_domip = gf_read_parquet( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_ip" )
    df_degree_ratio=get_df_degree_ratio(g_domip)
    df_write_parquet(df_degree_ratio, "/Users/olaya/Documents/Master/TFM/output_fraud/df_degree_ratio")
    '''
    '''
     # SAVING g_dom_dom graph locally
     g_domip = gf_read_parquet( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_ip" )
     g_domdom = get_graph_domdom( g_domip ).persist()
     gf_write_parquet( g_domdom, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_domain" )
    '''

    # LOAD LOCALLY DATA
    gf_domip = gf_read_parquet( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_ip_all_180208" )
    df_degree_ratio = df_read_parquet( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/df_degree_ratio_all_180208" )
    gf_domdom = gf_read_parquet( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_domain_all_180208" )

    # g_domip=gf_read_parquet( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_ip" )
    # df_degree_ratio = get_df_degree_ratio( g_domip )
    # df_write_csv( df_degree_ratio,"/Users/olaya/Documents/Master/TFM/output_fraud/df_degree_ratio_csv_prueba")
    # 'CSV data source does not support struct<id:string> data type.;'


    print( "plots_main MAIN-- calculando doms_more_visited : show todo.." )
    sorted_degrees = gf_top_most_visited(gf_domip,4)
    sorted_degrees.show()


    ### TODO : REVISAR

    print( "plots_main MAIN-- Calculando primer histograma --  draw_log_hist -- ..." )
    total_degrees = gf_domip.degrees
    print( f" type  {type( total_degrees )}  " )
    sorted_degrees = total_degrees.orderBy( F.desc( "degree" ) )

    degree, id = zip(*[(item.degree, item.id) for item in sorted_degrees.select( "degree", "id" ).collect()] )

    draw_log_hist( degree, [1, 10, 50, 100, 200, 300, 400],"/Users/olaya/Documents/Master/TFM/output_fraud/log_hist_plot.png" )


    print( "plots_main MAIN-- Calculando segundo histograma --  draw_minor_than_list -- ..." )


    list_tope = [400, 300, 200, 100, 50, 10]
    draw_minor_than_list( degree, list_tope,"/Users/olaya/Documents/Master/TFM/output_fraud/minor_than_list_plot.png" )



    print( "plots_main MAIN-- Calculando segundo histograma --  draw_overlap_matrix  -- list_top_suspicious ..." )
    top50_suspicious = df_degree_ratio.filter(
        "edge_ratio>0.5 and count_ips_in_common>1  " ).select(
        df_degree_ratio.a.id, df_degree_ratio.outDegree ).distinct().sort( F.desc( "outDegree" ) ).take( 50 )

    list_top_suspicious = [row["a.id"] for row in top50_suspicious]
    draw_overlap_matrix( df_degree_ratio, list_top_suspicious,"/Users/olaya/Documents/Master/TFM/output_fraud/overlap_list_top_suspicious.pdf" )

    print( "plots_main MAIN-- Calculando segundo histograma --  draw_overlap_matrix  -- list_sample ..." )
    sample_50_percent = df_degree_ratio.select( F.col( "a.id" ) ).distinct().sample( 0.2 ).take( 50 )

    # cojo un 20% de datos totales (dominios unicos) de la muestra de manera aleatoria
    list_sample = [row["id"] for row in sample_50_percent]
    draw_overlap_matrix( df_degree_ratio, list_sample,"/Users/olaya/Documents/Master/TFM/output_fraud/overlap_list_sample.pdf" )


    # Prueba de subgrafo
    #print( "plots_main MAIN-- Calculando subrafo para un domino especifico --  gf_filter_Edge  en gf_domip..." )
    #sub_gf_domip=(gf_domip,"yoorewards")
    #draw_nx( sub_gf_domip,"/Users/olaya/Documents/Master/TFM/output_fraud/sub_gf_domip_nx.png")

    #print( "plots_main MAIN-- Calculando subrafo para un domino especifico --  gf_filter_Edge  en gf_domdom..." )
    #sub_gf_domdom=(gf_domdom,"thegamer.com")
    #sub_gf_domdom.show()
    #ig, visual_style = draw_igraph_domain_domain( sub_gf_domdom )
    #plot( ig, **visual_style ).save(
    #    "/Users/olaya/Documents/Master/TFM/output_fraud/sub_gf_domdom_weighted.png" )



if __name__ == "__main__":
    main()
