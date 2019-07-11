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

from src.main.python.gf_utils.gf_utils import *


def print_show(df):
    print( "df_utils print_show --" )
    df.show()


def filter_string(s):
    # return isinstance( s, basestring )
    return type( s ) is str


def clean(df):
    print( "df_utils clean --" )

    udf_filter_string = udf( filter_string, BooleanType() )

    df = df.filter(
        (udf_filter_string( F.col( "referrer_domain" ) )) & (udf_filter_string( F.col( "user_ip" ) )) )

    # Añado una columna mas al df_current con el user_ip normalizado : ip_cleaned
    # Añado una columna mas al df_current con el referrer_domain normalizado : domain_cleaned

    udf_ipcleaner = udf( ip_cleaner, StringType() )
    print( "clean-- Calculando df_cleaned_ip" )
    df_cleaned_ip = df.withColumn( 'ip_cleaned', udf_ipcleaner( df.user_ip ) )

    udf_domaincleaner = udf( domain_cleaner, StringType() )
    df_cleaned = df_cleaned_ip.withColumn( 'domain_cleaned', udf_domaincleaner( df_cleaned_ip.referrer_domain ) )

    ## DROP/Filter Format not valid in ip_cleaned
    df_cleaned_format = df_cleaned.filter( (df_cleaned.ip_cleaned != 'Format not valid') )

    return df_cleaned_format

def format_vertices(df, a):
    """
    Rename columns of  df_vertices to use GrapFrames [id]
    :param df: df_vertices
    :param a:old name for column id
    :return: df_vertices

    """
    print( "df_utils format_vertices --" )
    df_vertices = df.select( col( "a" ).alias( "id" ) )

    return df_vertices


def format_edges(df, a, b, c): ##no funciona para DomainIp
    '''
     To rename the columns of df_edges in the correct format to GraphFrames [src, dst, edge_weight]
    :param df: df_edges with the incorrect name columns
    :param a: old name for column src
    :param b: old name for column dst
    :param c: old name for column edge_weight
    :return: df_edges
    '''
    print ("df_utils format_edges --")
    df_edges = df.select(
        col( "a" ).alias( "src" ), col( "b" ).alias( "dst" ),
        col( "c" ).alias( "edge_weight" ) )

    return df_edges


def draw_nx (df_edges):
    """

    :param df_edges: df_edges from a GraphFrame
    :return:
    """
    print ("df_utils draw_nx --")

    df = df_edges.toPandas()  ##GUARRADA

    B = nx.Graph()
    print ( "draw -- despues nx.Graph()")

    B.add_nodes_from(df['src'], bipartite=1)
    print ( "draw -- despues add_nodes_from src")

    B.add_nodes_from(df['dst'], bipartite=0)
    print ( "draw -- despues add_nodes_from dst")
    B.add_weighted_edges_from(
        [(row['src'], row['dst'], 1) for idx, row in df.iterrows()],
        weight='weight')
    print ( "draw -- Nodes added to B")

    print(B.edges(data=True))
    # [('test1', 'example.org', {'weight': 1}), ('test3', 'example.org', {'weight': 1}), ('test2', 'example.org', {'weight': 1}),
    # ('website.com', 'else', {'weight': 1}), ('site.com', 'something', {'weight': 1})]

    pos = {node:[0, i] for i,node in enumerate(df['src'])}
    pos.update({node:[1, i] for i,node in enumerate(df['dst'])})
    nx.draw(B, pos, with_labels=False)
    for p in pos:  # raise text positions
        pos[p][1] += 0.25
    nx.draw_networkx_labels(B, pos)

    plt.show()