from pyspark.sql import SparkSession
import pandas as pd

from utils.gf_utils import *
from utils.df_utils import *
from utils.draw_utils import *
from utils.spark_utils import *
from utils.read_write_utils import *



def main():
    '''Program entry point

    :return gf : graph graphframe Domain-IP bipartite relation
    '''

    # Intialize a spark context
    # with F.SparkContext( "local", "PySparCreateDataframe" ) as spark:
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
    spark = spark_session()
    #df = spark.createDataFrame( data )
    df = spark.read.format( "csv" ).option( "header", 'true' ).option( "delimiter", ',' ).load(
        "/Users/olaya/Documents/Master/TFM/Datos/180208/ssp_bid_compressed_00000000049*.csv.gz" )

    print( "DomainIpGraph MAIN-- Pintamos Dataframe completo ..." )
    # df.show()

    print( "DomainIpGraph MAIN-- clean ..." )
    df_cleaned = clean( df,"referrer_domain","user_ip" )
    print( "DomainIpGraph MAIN--cleaned df ..." )
    # df.show()
    print( "DomainIpGraph MAIN--get graph DI ... with a filter where the nodes with less than 15 visits: " )
    gf_domip = get_graph_domip( df_cleaned, 15 )

    # Completed - SAVED graph builded with 5 files of 200M day date 180208
    ##gf_write( gf_domip, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_ip_all_180208" )


    print( "DomainIpGraph MAIN-- Draw using igraph  OJO CAMBIaR ESTO..." )
    ig, visual_style = draw_igraph_domain_ip( gf_domip )
    plot( ig, **visual_style )


    ##draw_bp_igraph(gf) # FALTA POR HACER
    #print( "main -- Draw using nx.Graph -- all graph:" )
    #draw_nx( gf_domip.edges )

    print( "main -- Draw using nx.Graph -- only the nodes with more than 80 visits:" )
    df_domip_to_print = gf_domip.edges.filter( gf_domip.edges.edge_weight > 80 )
    draw_nx( df_domip_to_print )

    return gf_domip


if __name__ == "__main__":
    main()
