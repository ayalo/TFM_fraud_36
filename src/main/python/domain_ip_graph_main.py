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

    '''
    Code to generate an small dataframe called data with an specific structure
    '''
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
    '''
    # Intialize a spark context
    spark = spark_session()

    #code to load the small dataframe called data, to use uncomment the followin line.
    #df = spark.createDataFrame( data )

    '''
    Code to read a dataframe from one file saved on disk
    '''
    df = spark.read.format( "csv" ).option( "header", 'true' ).option( "delimiter", ',' ).load(
        "/Users/olaya/Documents/Master/TFM/Datos/180208/ssp_bid_compressed_00000000049[0-3].csv.gz" )

    print( "DomainIpGraph MAIN-- Print complete Dataframe ..." )
    df.show()

    # Always first clean an normalize de data
    print( "DomainIpGraph MAIN--  cleanning dataframe ..." )
    df_cleaned = clean( df,"referrer_domain","user_ip" )
    print( "DomainIpGraph MAIN--cleaned df ..." )
    df.show(15,False)

    # Generating the graphframe domain-ip
    print( "DomainIpGraph MAIN--get graph DI ... with a filter where the nodes with less than 80 visits: " )
    gf_domip = get_graph_domip( df_cleaned, 80 )

    # print( "main -- Draw using nx.Graph -- only the nodes with more than 80 visits:" )
    df_domip_to_print = gf_domip.edges.filter( gf_domip.edges.edge_weight > 80 )
    gf_domip_to_print = gf_filter_dom_ip_edges( gf_domip, 80 )

    # code to obtain the igraph to plot from the Graphframe gf_domip
    print( "DomainIpGraph MAIN-- Draw using igraph ..." )
    ig, visual_style = draw_igraph_domain_ip( gf_domip )
    plot( ig, **visual_style )

    # Code to plot and save the graph into disk
    plot( gf_domip, **visual_style ).save(
     "/Users/olaya/Documents/Master/TFM/output_fraud/gf_domip.png" )

    # code to plot the graph
    print( "main -- Draw using nx.Graph -- all graph:" )
    draw_nx( gf_domip.edges )

    # Code to plot and save the graph into disk
    draw_nx( gf_domip.edges,"/Users/olaya/Documents/Master/TFM/output_fraud/gf_domip_nx.png")

    return gf_domip


if __name__ == "__main__":
    main()
