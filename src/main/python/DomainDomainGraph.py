from pyspark.sql import SparkSession
import pandas as pd
from graphframes import *
from pyspark.sql import functions as F

from src.main.python.DomainIpGraph import get_graph_domip  ##como quito esto

from gf_utils.gf_utils import *
from df_utils.df_utils import *
from draw_utils.draw_utils import *


def get_graph_domdom(g_domip):
    """
    Get GraphFrame to draw graph
    :param g_domip: graph from domain_ip_graph .
    :return: gf (GraphFrame graph) domain-domain

    :definition df_vertices: vertices for the graphframe : domains
    :definition df_edges: links between them
    """

    ## meter excepcion cuando no hay ningun dominio - dominio relacionado para q no calcule todo
    print( "DomainDomainGraph get_graph_domdom -- g_domip.edges.show() " )
    g_domip.edges.show()
    # Query  DomainIpGraph para obtener interseccion de IPs visitadas por 2 dominios distintos
    df_motifs = g_domip.find( "(a)-[e]->(b); (c)-[e2]->(b)" ).filter( "a != c" ).dropDuplicates( ['e', 'e2'] )
    print( "DomainDomainGraph get_graph_domdom -- df_motifs " )
    df_motifs.show()

    df_motifs_count = df_motifs.groupBy( 'a', 'c' ).agg( F.count( F.col( "b" ) ).alias( "count_ips_in_common" ) )
    print( "DomainDomainGraph get_graph_domdom -- df_motifs_count " )
    df_motifs_count.show()

    outDeg = g_domip.outDegrees
    print( "DomainDomainGraph get_graph_domdom --  df_motifs_count : " )
    # df_motifs.show(6,False)

    print( "DomainDomainGraph get_graph_domdom --  df_degreeRatio : " )
    df_degree = df_motifs_count.join( outDeg, df_motifs_count.a.id == outDeg.id )
    df_degree.show( 10, False )
    # print( df_degree.schema )

    df_degree_ratio = df_degree.withColumn( 'edge_ratio', df_degree.count_ips_in_common / df_degree.outDegree )
    print( "DomainDomainGraph get_graph_domdom --  df_degreeRatio division: " )
    df_degree_ratio.show( 10, False )
    # print( df_degree_ratio.schema )

    df_edges = get_edges_domdom( df_degree_ratio )
    df_vertices = get_vertices( df_edges, "src", "dst" )
    print( "DomainDomainGraph get_graph_domdom --  df_vertices removed duplicates : " )

    ##g = GraphFrame(df_vertices_index, df_edges) # de cuando añadiamos columna 'id'  y no renombrabamos 'domain'
    gf = GraphFrame( df_vertices, df_edges )

    return gf


def get_edges_domdom(df):
    """
    Creating a df_edges to use GraphFrames
    :param df: dataframe from our data. Idem format like in get_vertices function.
    :return: df_edges
    """
    print( "DomainDomainGraph get_edges -- df que llega ..." )
    df.show()
    df.printSchema()

    df_edges_DD_exists = df.select( df.a, df.c,
                                    F.when( df['edge_ratio'] > 0.5, 1 ).otherwise( 0 ).alias(
                                        "edge_ratio" ) )  # .show()

    df_edges_DD_exists.printSchema()
    ##df_edges=df_edges_DD_exists.toDF("src","dst","edge_weight")
    # df_edges = df_edges_DD_exists.select(
    #    col( "a" ).alias( "src" ), col( "c" ).alias( "dst" ),
    #    col( "edge_ratio" ).alias( "edge_weight" ) )  # .persist()
    print( "DomainDomainGraph get_edges -- antes get_edges ..." )

    df_edges = get_edges( df_edges_DD_exists, "a", "c", "edge_ratio" )
    print( "DomainDomainGraph get_edges -- df_edges calculado ..." )
    df_edges.show()

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
                             'example.org',
                             'website.com'],
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
                     '30.50.70.90',
                     '10.20.30.40']} )
    # 'subdomain': ['test1', 'something', 'test2', 'test3', 'else', 'else', 'else', 'else', 'else', 'else']} )
    spark = SparkSession.builder.getOrCreate()
    # df = spark.createDataFrame( data )
    df = spark.read.format( "csv" ).option( "header", 'true' ).option( "delimiter", ',' ).load(
        "/Users/olaya/Documents/Master/TFM/Datos/ssp_bid_compressed_000000000499.csv.gz" )

    print( "DomainDomainGraph MAIN -- Pintamos Dataframe completo:" )
    # df.show()
    # print_show( df )

    df = clean( df )
    # g= src.main.python.DomainIpGraph.get_graph(df)
    g_domip = get_graph_domip( df, 15 ).persist()
    print( "DomainDomainGraph MAIN -- triplets " )
    #g_domip.triplets.show( 100, False )

    gf = get_graph_domdom( g_domip )

    draw_igraph( gf )
    draw_nx( gf.edges )

    print( "DomainDomainGraph MAIN -- Show only connected components" )
    spark.sparkContext.setCheckpointDir( 'DomainDomainGraph_cps' )
    # components=gf.connectedComponents().sort()# ## necesita una carpeta para cps
    components = gf.stronglyConnectedComponents( maxIter=10 ).select( "id", "component" ).groupBy( "component" ).agg(
        F.count( "id" ).alias( "count" ) ).orderBy( desc( "count" ) )


if __name__ == "__main__":
    main()
