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

    # df = spark.read.format( "csv" ).option( "header", 'true' ).option( "delimiter", ',' ).load(
    # "/Users/olaya/Documents/Master/TFM/Datos/180208/ssp_bid_compressed_000000000491.csv.gz" )
    df = (spark.read.csv( "/Users/olaya/Documents/Master/TFM/Datos/180208/ssp_bid_compressed_000000000499.csv.gz",
                          header="true", timestampFormat="yyyy-MM-dd HH:mm:ss", escape='"',
                          ignoreLeadingWhiteSpace="true", ignoreTrailingWhiteSpace="true", mode="FAILFAST" ).select(
        "user_ip", "referrer_domain" ))

    print( "DomainDomainGraph MAIN -- Pintamos Dataframe completo:" )
    # df.show()
    # print_show( df )
    print( "DomainDomainGraph MAIN-- cleanning dataframe ..." )
    df_cleaned = clean( df, "referrer_domain", "user_ip" )

    print( "DomainDomainGraph MAIN--get graph DI ... with a filter where the nodes with less than 15 visits: " )
    gf_domip = get_graph_domip( df_cleaned, 10 )
    print( "DomainDomainGraph MAIN -- triplets " )
    # gf_domip.triplets.show( 100, False )

    gf_domdom = get_graph_domdom( gf_domip )  # .persist()

    ig, visual_style = draw_igraph_domain_domain( gf_domdom )
    # plot( ig, visual_style)

    plot( ig, **visual_style ).save(
        "/Users/olaya/Documents/Master/TFM/output_fraud/gf_domdom_499.png" )


if __name__ == "__main__":
    main()
