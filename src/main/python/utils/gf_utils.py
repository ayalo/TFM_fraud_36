from graphframes import *

from utils.df_utils import *
from pyspark.sql import functions as F


def gf_filter_dom_ip_edges(g, min_edge):  # filtar count>15 visitas  en el grafo. # Usada en DI y DD
    """
    Function to select only the nodes in the graph with more than a given weight passed as parameter, in order
    to filter non relevant data to construct an smaller graph .
    :param g: original gf_dom_ip GraphFrame
    :param min_edge: value to dismiss all the nodes on g below the limit_edge value . limit_edge value indicate the
            number of visits domain-ip (if for a src - dst tuple : edge_weight <  limit_edge this row is discarded)
    :return gf_filtered: GraphFrame generated with
    """

    print( "gf_utils filter_gf filterEdges : {min_edge}" )  ## hace lo mismo que g.edges.filter( "edge_weight > '10' " )
    g_edges_filtered = g.filterEdges( f"edge_weight > {min_edge}" )
    # g_edges_filtered.edges.show()

    return g_edges_filtered


def get_graph_domip(df, min_edge):
    """
    Get GraphFrame to draw a bipartite graph
    :param df dataframe from our data. Idem format like in get_vertices function.
    :param min_edge: value to dismiss all the nodes on g below the min_edge value
                        (if for a src - dst tuple : edge_weight <  min_edge this row is discarded)
    :return: gf_filtered (GraphFrame graph) filtered by min_edge

    :definition df_vertices: vertices for the graphframe : domains and ips
    :definition df_edges: links between them
    """
    print( "gf_utils get_graph_domip-- :" )

    df_edges = get_edges_domip( df )  # .persist()
    df_vertices = get_vertices( df, "domain_cleaned", "ip_cleaned" )  # .persist()

    gf = GraphFrame( df_vertices, df_edges )  # get_graph(df_vertices,df_edges)

    gf_filtered = gf_filter_dom_ip_edges( gf, min_edge )

    return gf_filtered


def get_motifs(g_domip):
    """
    Query graph to get motifs df of ip_cleaned IPs visited by 2 different domain_cleaned domains

        Motifs finding is also known as graph pattern matching.
        The pattern matching consists on checking a value against some pattern.
        The pattern is an expression used to define some connected vertices.

    :param g_domip:  graphframe
    :return: df_motifs
    """
    # Query  DomainIpGraph to obtain the visited IPs intersection for 2 diferent domains
    print( "gf_utils get_motifs -- df_motifs dropDuplicates( ['e', 'e2'] " )
    df_motifs = g_domip.find( "(a)-[e]->(b); (c)-[e2]->(b)" ).filter( "a != c" ).dropDuplicates( ['e', 'e2'] )
    # df_motifs.show()

    return df_motifs


def get_motifs_count(df_motifs):
    """
    Get the count of ip_cleaned IPs visisted by 2 different domain_cleaned domains

    :param df_motifs:
    :return: df_motifs_count
    """
    df_motifs_count = df_motifs.groupBy( 'a', 'c' ).agg( F.count( F.col( "b" ) ).alias( "count_ips_in_common" ) )
    print( "gf_utils get_motifs_count -- df_motifs_count " )
    # df_motifs_count.show()

    return df_motifs_count


def get_df_degree_ratio(g_domip):
    """
    Function to get the df_degree_ratio, with the format described below.
    :param g_domip:
    :return df_degree_ratio : dataframe with all the data needed to represent the overlap matrix
    [a,c,count_ips_in_common,id,outDegree,edge_ratio]
    where a is src, c is dst, id is src, and outDegree is the outDegree of src.
    The edge_ratio is calculated with the algorithm proposed.
    """
    outDeg = g_domip.outDegrees

    df_motifs_count = get_motifs_count( get_motifs( g_domip ) )

    print( "gf_utils get_df_degree_ratio --  df_degreeRatio : " )
    df_degree = df_motifs_count.join( outDeg, df_motifs_count.a.id == outDeg.id )
    # df_degree.show( 10, False )

    df_degree_ratio = df_degree.withColumn( 'edge_ratio',
                                            df_degree.count_ips_in_common / (df_degree.outDegree + '0.000000001') )
    #print( "gf_utils get_df_degree_ratio --  df_degreeRatio division  (edge_ratio = covisitation degree: " )
    # df_degree_ratio.show( 10, False )

    return df_degree_ratio


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
    df_degree_ratio = get_df_degree_ratio( g_domip )
    # df_degree_ratio.show(10,False)

    df_edges = get_edges_domdom( df_degree_ratio )
    df_vertices = get_vertices( df_edges, "src", "dst" )
    #print( "DomainDomainGraph get_graph_domdom --  df_edges  gf_total_nodes OLAYAs : " )
    #df_edges.show()

    gf_total_nodes = GraphFrame( df_vertices, df_edges )

    df_edges = get_edges_domdom_malicious_ones( df_degree_ratio )
    df_vertices = get_vertices( df_edges, "src", "dst" )
    #print( "DomainDomainGraph get_graph_domdom --  df_edges gf_malicious_nodes OLAYAs : " )
    #df_edges.show()

    gf_malicious_nodes = GraphFrame( df_vertices, df_edges )

    return gf_total_nodes, gf_malicious_nodes


def gf_top_most_visited(gf, top=None):
    """
    Using the graphframe function 'degrees', calculate the most visited vertices.
    :param gf:  grapframe
    :return: sorted_degrees
    """
    # hacerlo tb con el grafo dom-ip get_outDegree y sacar un ranking
    # ojo utilizar cleaned dataframe

    total_degrees = gf.degrees
    print( f" type  {type( total_degrees )}  " )
    sorted_degrees = total_degrees.orderBy( F.desc( "degree" ) )
    print( "gf_utils gf_top_most_visited -- sorted_degrees.show .. " )

    # TODO VER COMO SE PUEDE IMPRIMIR o revisar o quitar
    # top20_outDegrees=outDegree.sort( "outDegree", ascending=False ).head(20)
    if top is None:
        return sorted_degrees
    return sorted_degrees.limit( top )


# TODO def doms_or_ip_more_visited(gf_dom_ip):

def gf_filter_Edge(gf, src):
    """
    Function to get the subgraph of Graphframes graph related of a src (domain) gived as a function parameter
    :param gf:
    :param src:
    :return: subgraph for the src
    """
    return gf.filterEdges( f"src='{src}'" )
