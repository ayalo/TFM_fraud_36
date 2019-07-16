from pyspark.sql import SparkSession
import pandas as pd

from utils.gf_utils import *
from utils.df_utils import *
from utils.draw_utils import *
from utils.spark_utils import *


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
        "/Users/olaya/Documents/Master/TFM/Datos/ssp_bid_compressed_000000000499.csv.gz" )

    print( "DomainIpGraph MAIN-- Pintamos Dataframe completo ..." )
    # df.show()

    print( "DomainIpGraph MAIN-- clean ..." )
    df_cleaned = clean( df,"referrer_domain","user_ip" )
    print( "DomainIpGraph MAIN--cleaned df ..." )
    # df.show()
    print( "DomainIpGraph MAIN--get graph DI ... " )
    gf = get_graph_domip( df_cleaned, 10 )

    print( "DomainIpGraph MAIN-- Draw using igraph ..." )
    draw_igraph( gf )

    print( "main -- Draw using nx.Graph :" )
    draw_nx( gf.edges )

    return gf


if __name__ == "__main__":
    main()
