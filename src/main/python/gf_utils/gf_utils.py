from graphframes import *

from df_utils.df_utils import *


def filter_gf(g, min_edge):  # filtar count=1 en el grafo. Devuelve un grafo # Usada en DI y DD
    """
    Function to select only the nodes in the graph with more than a given weight passed as parameter, in order
    to filter non relevant data to construct an smaller graph .
    :param g: original GraphFrame
    :param min_edge: value to dismiss all the nodes on g below the limit_edge value
                        (if for a src - dst tuple : edge_weight <  limit_edge this row is discarded)
    :return gf_filtered: GraphFrame generated with
    """

    print(
        f"gf_utils filter_gf filterEdges : {min_edge}" )  ## hace lo mismo que g.edges.filter( "edge_weight > '10' " )
    g_edges_filtered = g.filterEdges( f"edge_weight > {min_edge}" )
    g_edges_filtered.edges.show()

    return g_edges_filtered

'''
def get_graph(df_v, df_e):  ## Creo que no se usa . ## no funciona con DomainIp, no se usa en DomainDomain
    """
    Get GraphFrame to generate a GraphFrame graph
    :param df_v : dataframe of vertices format [id]
    :param df_e : datagrame of edges format [src,dst,edge_weight]
    :return: gf (GraphFrame graph)
    """
    print( "gf_utils get_graph --" )

    df_vertices = format_vertices( df_v )
    df_edges = get_edges( df_e )
    gf = GraphFrame( df_vertices, df_edges )
    
    return gf
'''

def get_graph_domip(df, min_edge):
    """
    Get GraphFrame to draw a bipartite graph
    :param df dataframe from our data. Idem format like in get_vertices function.
    :param min_edge: value to dismiss all the nodes on g below the limit_edge value
                        (if for a src - dst tuple : edge_weight <  limit_edge this row is discarded)
    :return: gf_filtered (GraphFrame graph) filtered by min_edge

    :definition df_vertices: vertices for the graphframe : domains and ips
    :definition df_edges: links between them
    """
    print( "DomainIpGraph get_graph_DI-- :" )

    df_edges = get_edges_domip( df ).persist()
    df_vertices = get_vertices( df, "domain_cleaned", "ip_cleaned" ).persist()

    gf = GraphFrame( df_vertices, df_edges )  # get_graph(df_vertices,df_edges)

    gf_filtered = filter_gf( gf, min_edge )

    return gf_filtered

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
