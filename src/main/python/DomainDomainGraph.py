from pyspark.sql import SparkSession
import pandas as pd
from graphframes import *

from igraph import *
import networkx as nx
import matplotlib.pyplot as plt

from src.main.python.DomainCleaner import domain_cleaner
from src.main.python.IpCleaner import ip_cleaner

from pyspark.sql.functions import udf
from pyspark.sql.functions import col

from pyspark.sql import functions as F
from pyspark.sql.types import *

from src.main.python.DomainIpGraph import get_graph_DI

from src.main.python.gf_utils.gf_utils import *
from src.main.python.df_utils.df_utils import *


def get_graph(df):
    """
    Get GraphFrame to draw a bipartite graph
    :param df dataframe from our data. Idem format like in get_vertices function.
    :return: gf (GraphFrame graph)

    :definition df_vertices: vertices for the graphframe : domains
    :definition df_edges: links between them
    """

    df_vertices = get_vertices( df )
    df_edges = get_edges( df )
    ##g = GraphFrame(df_vertices_index, df_edges) # de cuando añadiamos columna 'id'  y no renombrabamos 'domain'
    gf = GraphFrame( df_vertices, df_edges )

    return gf


def get_vertices(df):
    """
    Creating a df_vertices to use GrapFrames
    :param df:
    :return: df_vertices

    """
    print( "DomainDomainGraph get_vertices" )
    df_vertices = df.select( col( "a" ).alias( "id" ) )
    return df_vertices


def get_edges(df):
    """
    Creating a df_edges to use GraphFrames
    :param df: dataframe from our data. Idem format like in get_vertices function.
    :return: df_edges
    """
    print( "DomainDomainGraph get_edges" )
    df.show()
    df.printSchema()
    df_edges_DD_exists = df.select( df.a, df.c,
                                    F.when( df['edge_ratio'] > 0.5, 1 ).otherwise( 0 ).alias("edge_ratio") )  # .show()

    df_edges_DD_exists.printSchema()
    ##df_edges=df_edges_DD_exists.toDF("src","dst","edge_weight")
    df_edges = df_edges_DD_exists.select(
        col( "a" ).alias( "src" ), col( "c" ).alias( "dst" ),
        col( "edge_ratio" ).alias( "edge_weight" ) )  # .persist()

    return df_edges


def main():
    '''Program entry point'''

    # Intialize a spark context
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
    spark = SparkSession.builder.getOrCreate()
    ##df = spark.createDataFrame( data )
    df = spark.read.format( "csv" ).option( "header", 'true' ).option( "delimiter", ',' ).load(
        "/Users/olaya/Documents/Master/TFM/Datos/ssp_bid_compressed_000000000499.csv.gz" )

    print( "DomainDomainGraph MAIN -- Pintamos Dataframe completo:" )
    # df.show()
    #print_show( df )

    df = clean( df )
    # g= src.main.python.DomainIpGraph.get_graph(df)
    g = get_graph_DI( df ).persist()
    #print ("DomainDomainGraph MAIN -- triplets ")
    #g.triplets.show(100,False)

    # Query  DomainIpGraph para obtener interseccion de IPs visitadas por 2 dominios distintos
    df_motifs = g.find( "(a)-[e]->(b); (c)-[e2]->(b)" ).filter( "a != c" ).dropDuplicates( ['e', 'e2'] )
    print ("DomainDomainGraph MAIN -- df_motifs ")
    df_motifs.show()

    df_motifs_count = df_motifs.groupBy( 'a', 'c' ).agg( F.count( F.col( "b" ) ).alias( "count_ips_in_common" ) )
    print ("DomainDomainGraph MAIN -- df_motifs_count ")
    df_motifs_count.show()

    outDeg = g.outDegrees
    print( "DomainDomainGraph MAIN --  df_motifs_count : " )
    # df_motifs.show(6,False)

    print( "DomainDomainGraph MAIN --  df_degreeRatio : " )
    df_degree = df_motifs_count.join( outDeg, df_motifs_count.a.id == outDeg.id )
    df_degree.show(10,False)
    # print( df_degree.schema )

    df_degree_ratio = df_degree.withColumn( 'edge_ratio', df_degree.count_ips_in_common / df_degree.outDegree )
    print( "DomainDomainGraph MAIN --  df_degreeRatio division: " )
    df_degree_ratio.show(10,False)
    # print( df_degree_ratio.schema )

    gf = get_graph( df_degree_ratio )
    draw_igraph( gf )
    #draw_nx(get_edges(df_degree_ratio))


if __name__ == "__main__":
    main()
