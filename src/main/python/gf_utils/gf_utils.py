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

from src.main.python.df_utils.df_utils import *


def get_graph(df_v, df_e):  ## no funciona con DomainIp
    """
    Get GraphFrame to draw a bipartite graph
    :param df_v : dataframe of vertices
    :param df_e : datagrame of edges
    :return: gf (GraphFrame graph)
    """
    print( "gf_utils get_graph --" )

    df_vertices = format_vertices( df_v )
    df_edges = format_edges( df_e )
    gf = GraphFrame( df_vertices, df_edges )
    return gf


def filter_gf(g):  # filtar count=1 en el grafo. Devuelve un grafo
    print( "gf_utils filter_gf --" )

    num_counter = g.edges.filter( "edge_weight > '15'" ).count()
    print( "The number of follow edges edge_weight > 15  is", num_counter )

    df_edges_filtered = g.edges.filter( "edge_weight > '15'" )

    return df_edges_filtered


def draw_igraph(g):
    """
    :param g:
    :return:
    """
    print( "gf_utils draw_igraph --" )

    ig = Graph.TupleList( g.edges.collect(), directed=True )
    print( "draw_igraph-- despues ---" )
    plot( ig )
