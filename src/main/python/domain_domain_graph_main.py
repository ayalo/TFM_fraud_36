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
        "/Users/olaya/Documents/Master/TFM/Datos/180208/ssp_bid_compressed_00000000049[0-3]*.csv.gz" )
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

    # To locally write the data of a graphframe gf_domip, to use, uncomment the following line with ##
    ##gf_write_parquet( gf_domip, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_ip_491_09092019" )

    # To locally read the data of a graphframe gf_domip, to use, uncomment the following line with ##
    ##gf_domip = gf_read_parquet( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_ip_all_180208" )
    ##gf_domip = gf_read_parquet( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_ip_491_09092019" )

    # The GraphFrames triplets put all of the edge, src, and dst columns together in a DataFrame.
    print( "DomainDomainGraph MAIN -- triplets " )
    gf_domip.triplets.show( 100, False )

    # code to get the Graphframe domain-domain :
    # gf_domdom_total : contains all the domain in the dataset
    # gf_domdom_malicious : contains only the domain catalogues as malicious ones by the algorithm 
    gf_domdom_total, gf_domdom_malicious = get_graph_domdom( gf_domip )  # .persist()

    print( "DomainDomainGraph MAIN-- gf_domdom_total.edges.show: " )
    gf_domdom_total.edges.show( 10, False )
    print( "DomainDomainGraph MAIN-- gf_domdom_malicious.edges.show: " )
    gf_domdom_malicious.edges.show( 10, False )

    # code to locally save the malicious graph gf_domdom into disk
    gf_write_parquet( gf_domdom_total, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_domain_total_0-3_11102019" )
    gf_write_parquet( gf_domdom_malicious, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_domain_malicious_0-3_11102019" )

    # code to locally read the malicious graph gf_domdom from disk
    ##gf_domdom_malicious= gf_read_parquet( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_domain_malicious_0-3_11102019" )
    ##gf_domdom_malicious.edges.show( 10, False )

    # code to obtain the igraph to plot from the Graphframe gf_domdom_total
    ig, visual_style = draw_igraph_domain_domain( gf_domdom_total )
    # code to obtain the igraph to plot from the Graphframe gf_domdom_malicious
    ig_mal, visual_style_mal = draw_igraph_domain_domain( gf_domdom_malicious )

    # Code to plot the graph
    ## plot( ig, **visual_style )

    # Code to plot and save the graph into disk
    plot( ig, **visual_style ).save(
        "/Users/olaya/Documents/Master/TFM/output_fraud/gf_domdom_total__0-3_10092019.png" )
    print( "DomainDomainGraph MAIN-- gf_domdom_malicious ig saving into png: " )
    plot( ig_mal, **visual_style_mal).save(
       "/Users/olaya/Documents/Master/TFM/output_fraud/gf_domdom_malicious_0-3_10092019.png" )

    # code to obtain the igraph to plot from the Graphframe gf_domdom_malicious
    ##ig, visual_style = draw_igraph_domain_domain( gf_domdom_malicious )

    # code to plot the graph
    ##plot( ig, **visual_style )

    '''
    Code to generate a subgraph of a malicious domain, given a previosly calculate gf_dom_dom_malicious
    '''
    print( "DomainDomainGraph MAIN-- gf_domdom_malicious trying to obtain a subgraph with specific domain: " )

    sub_graph_malicious_domain_1 = gf_domdom_malicious.filterEdges( "src.id = 'myyearbook.m'")
    ig1, visual_style1 = draw_igraph_domain_domain( sub_graph_malicious_domain_1 )
    plot( ig1, **visual_style1).save( "/Users/olaya/Documents/Master/TFM/output_fraud/gf_domdom_myyearbook.m_11102019.png" )
    #print( "main -- Draw using nx.Graph -- all graph:" )
    #draw_nx( sub_graph_malicious_domain_1.edges, "/Users/olaya/Documents/Master/TFM/output_fraud/gf_domdom_dealnews.com_09092019_nx.png" )
    #sub_graph_malicious_domain_2 = gf_domdom_malicious.filterEdges( "src = 'therichest.com'")
    #ig2, visual_style2 = draw_igraph_domain_domain( sub_graph_malicious_domain_2 )
    #plot( ig2, **visual_style2 ).save(
    #    "/Users/olaya/Documents/Master/TFM/output_fraud/gf_domdom_therichest_09092019.png" )

    '''
    Code to read a gf_domdom, previosly saved on disk, and generate the visual graph saved into disk
    '''
    '''
    gf_domdom = gf_read_parquet( spark, "/Users/olaya/Documents/Master/TFM/output_fraud/graph_domain_domain_weighted" )
    ig, visual_style = draw_igraph_domain_domain( gf_domdom )
    plot( ig, **visual_style ).save(
        "/Users/olaya/Documents/Master/TFM/output_fraud/gf_domdom_weighted_rounded.png" )
    '''

if __name__ == "__main__":
        main()
