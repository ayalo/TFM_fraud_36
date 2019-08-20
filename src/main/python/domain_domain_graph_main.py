import pandas as pd

from utils.gf_utils import *
from utils.df_utils import *
from utils.draw_utils import *
from utils.spark_utils import *
from utils.read_write_utils import *


def main():
    '''Program entry point'''

    # Intialize a spark context
    spark = spark_session()

    '''
    Code to read a dataframe from one file saved on disk, option 1
    to use, uncomment the following 2 lines with ##
    '''
    df = spark.read.format( "csv" ).option( "header", 'true' ).option( "delimiter", ',' ).load(
        "/Users/olaya/Documents/Master/TFM/Datos/180208/ssp_bid_compressed_000000000491.csv.gz" )
    '''
    Code to read a dataframe from one file saved on disk, option 2
    to use, uncomment the following 4 lines with ##
    '''
    ## df = (spark.read.csv( "/Users/olaya/Documents/Master/TFM/Datos/180208/ssp_bid_compressed_000000000499.csv.gz",
    ##                      header="true", timestampFormat="yyyy-MM-dd HH:mm:ss", escape='"',
    ##                      ignoreLeadingWhiteSpace="true", ignoreTrailingWhiteSpace="true", mode="FAILFAST" ).select(
    ##    "user_ip", "referrer_domain" ))
    '''
    Code to read a daraframe from a set of files 
    '''
    '''
    df = spark.read.format( "csv" ).option( "header", 'true' ).option( "delimiter", ',' ).load(
        "/Users/olaya/Documents/Master/TFM/Datos/180208/ssp_bid_compressed_*.csv.gz" )
    '''


    print( "DomainDomainGraph MAIN -- Print complete Dataframe :" )
    df.show()
    
    # Always first clean an normalize de data
    print( "DomainDomainGraph MAIN-- cleanning dataframe ..." )
    df_cleaned = clean( df, "referrer_domain", "user_ip" )

    # Generating the graphframe domain-ip
    print( "DomainDomainGraph MAIN--get graph DI ... with a filter where the nodes with less than 15 visits: " )
    gf_domip = get_graph_domip( df_cleaned, 15 )

    # To locally read the data of a graphframe gf_domip, to use, uncomment the following line with ##
    ##gf_domip = gf_read_parquet( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_ip_all_180208" )


    #The GraphFrames triplets put all of the edge, src, and dst columns together in a DataFrame.
    print( "DomainDomainGraph MAIN -- triplets " )
    gf_domip.triplets.show( 100, False )

    # code to get the Graphframe domain-domain :
    # gf_domdom_total : contains all the domain in the dataset
    # gf_domdom_malicious : contains only the domain catalogues as malicious ones by the algorithm 
    gf_domdom_total,gf_domdom_malicious = get_graph_domdom( gf_domip )  # .persist()

    print( "DomainDomainGraph MAIN-- gf_domdom_total.edges.show: " )
    gf_domdom_total.edges.show( 10, False )
    print( "DomainDomainGraph MAIN-- gf_domdom_malicious.edges.show: " )
    gf_domdom_malicious.edges.show( 10, False )


    # code to save the graph gf_domdom into disk
    #gf_write_parquet( gf_domdom, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_domain_weighted" )

    # code to obtain the igraph to plot from the Graphframe gf_domdom_total
    ig, visual_style = draw_igraph_domain_domain( gf_domdom_total )

    # Code to plot the graph
    plot( ig, **visual_style )

    # Code to plot and save the graph into disk
    ##plot( ig, **visual_style ).save(
    ##    "/Users/olaya/Documents/Master/TFM/output_fraud/gf_domdom_491_total.png" )

    # code to obtain the igraph to plot from the Graphframe gf_domdom_malicious
    ig, visual_style = draw_igraph_domain_domain( gf_domdom_malicious )

    # code to plot the graph
    plot( ig, **visual_style )

    # Code to plot and save the graph into disk
    ##plot( ig, **visual_style ).save(
    ##    "/Users/olaya/Documents/Master/TFM/output_fraud/gf_domdom_491_malicious.png" )


    '''
    Code to read a gf_domdom, previosly saven on disk, and generate the visual graph saved into disk
    '''
    '''
    gf_domdom = gf_read_parquet( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_domain_weighted" )
    ig, visual_style = draw_igraph_domain_domain( gf_domdom )
    plot( ig, **visual_style ).save(
        "/Users/olaya/Documents/Master/TFM/output_fraud/gf_domdom_weighted_rounded.png" )
    '''

if __name__ == "__main__":
    main()
