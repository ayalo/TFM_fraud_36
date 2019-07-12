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


def get_graph(df_v, df_e):  ## no funciona con DomainIp, no se usa en DomainDomain
    """
    Get GraphFrame to generate a GraphFrame graph
    :param df_v : dataframe of vertices format [id]
    :param df_e : datagrame of edges format [src,dst,edge_weight]
    :return: gf (GraphFrame graph)
    """
    print( "gf_utils get_graph --" )

    df_vertices = format_vertices( df_v )
    df_edges = format_edges( df_e )
    gf = GraphFrame( df_vertices, df_edges )
    return gf


def filter_gf(g, min_edge):  # filtar count=1 en el grafo. Devuelve un grafo # Usada en DI y DD
    """
    Function to select only the nodes in the graph with more than a given weight passed as parameter, in order
    to filter non relevant data to construct an smaller graph .
    :param g: original GraphFrame
    :param min_edge: value to dismiss all the nodes on g below the limit_edge value
                        (if for a src - dst tuple : edge_weight <  limit_edge this row is discarded)
    :return gf_filtered: GraphFrame generated with
    """

    print(f"gf_utils prueba filterEdges : {min_edge}") ## hace lo mismo que g.edges.filter( "edge_weight > '10' " )
    g2=g.filterEdges(f"edge_weight > {min_edge}")
    g2.edges.show()

    num_counter = g.edges.filter( f"edge_weight > {min_edge}" ).count()
    print( f"The number of follow edges edge_weight > {min_edge}  is  {num_counter} " )

    df_edges_filtered = g.edges.filter( f"edge_weight > {min_edge}  " )
    print( "gf_utils filter_gf --" )
    df_edges_filtered.show()
    print( "DomainIpGraph get_graph_DI-- df_edges_src count : {} ".format( df_edges_filtered.select( "src" ).count() ) )

    print( "DomainIpGraph get_graph_DI-- df_edges_dst count : {} ".format( df_edges_filtered.select( "dst" ).count() ) )

## GUARRADA ··
    df_dom = df_edges_filtered.select( col( "src" ).alias( "id" ) )
    df_ip = df_edges_filtered.select( col( "dst" ).alias( "id" ) )
    df_vertices_filtered = df_dom.union( df_ip )

    print( "DomainIpGraph get_graph_DI-- df_vertices count : {} ".format( df_vertices_filtered.select( "id" ).count() ) )

    gf_filtered = GraphFrame( df_vertices_filtered, df_edges_filtered )#get_graph(df_vertices,df_edges_filtered)

    return gf_filtered


def draw_igraph(g):  ##usada en DI y DD
    """
    Function to plot a dispersion of nodes (TupleList) in a graph with igraph
    :param g: GraphFrame
    :return: ploted graph
    """
    print( "gf_utils draw_igraph --" )

    ig = Graph.TupleList( g.edges.collect(), directed=True )
    print( "draw_igraph-- despues ---" )
    plot( ig )
