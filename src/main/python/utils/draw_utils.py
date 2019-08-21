import matplotlib.pyplot as plt
from igraph import Graph
from igraph import plot
import networkx as nx
import pandas as pd
import numpy as np
import matplotlib.ticker as ticker
from pyspark.sql import functions as F
from pyspark.sql.functions import col



def draw_nx(df_edges,path=None):  # used in domain-ip  and dom-dom graphs, doesn't work with huge amount of data
    """
    Function to plot a bipartite graph with networkx
    :param df_edges: df_edges from a GraphFrame
    :param path=None
    :return: ploted graph

    """

    #import networkx as nx
    #import pandas as pd

    print( "draw_utils draw_nx --" )

    df = df_edges.toPandas()  ##GUARRADA

    B = nx.Graph()
    #print( "draw_utils draw_nx -- -- despues nx.Graph()" )

    B.add_nodes_from( df['src'], bipartite=1 )
    #print( "draw_utils draw_nx -- -- despues add_nodes_from src" )

    B.add_nodes_from( df['dst'], bipartite=0 )
    #print( "draw_utils draw_nx -- -- despues add_nodes_from dst" )
    #B.add_weighted_edges_from(
    #    [(row['src'], row['dst'], 1) for idx, row in df.iterrows()],
    #    weight=row[] )

    B.add_edges_from( zip( df['src'], df['dst'] ), weight=1 )

    #print( "draw_utils draw_nx -- -- Nodes added to B" )
    # print( B.edges( data=True ) )

    pos = {node: [0, i] for i, node in enumerate( df['src'] )}
    pos.update( {node: [1, i] for i, node in enumerate( df['dst'] )} )
    #print( "draw_utils draw_nx -- -- despues de pos.update" )
    nx.draw( B, pos, with_labels=False )
    for p in pos:  # raise text positions
        pos[p][1] += 0.20
    #print( "draw_utils draw_nx -- -- despues for " )
    nx.draw_networkx_labels( B, pos )

    #Probando margenes
    x_values, y_values = zip( *pos.values() )
    x_max = max( x_values )
    x_min = min( x_values )
    x_margin = (x_max - x_min) * 0.25
    y_max = max( y_values )
    y_min = min( y_values )
    y_margin = (y_max - y_min) * 0.7
    plt.xlim( x_min - x_margin, x_max + x_margin, y_min - y_margin, y_max + y_margin )
    #fin probando margenes

    if f"{path}" is not None:
        plt.savefig( f"{path}" )
    #print( "draw_utils draw_nx -- -- ante de plot" )
    plt.show()


def draw_igraph_domain_domain(g_domdom):
    """
    Version: 1.0
    Function to plot a dispersion of nodes (TupleList) in a graph with igraph
    :param g: GraphFrame
    :return: ig,visual_style : igraph and visual_style to draw the graph
    """

    #from igraph import Graph

    print( "draw_utils draw_igraph --" )
    edges = g_domdom.edges.select( F.col( "src.id" ).alias( "src" ), F.col( "dst.id" ).alias( "dst" ),
                                   F.col( "edge_weight" ) ).collect()
    # METER edge_weight a FLOAT round(a, 4)
    ig = Graph.TupleList( edges, directed=True, weights=True )
    visual_style = {}
    N_vertices = ig.vcount()

    #print( ig.es["weight"] )
    #print( f" is weighted {ig.is_weighted()} " )
    # layout = ig.layout( "kk" )
    # layout= layout_kamada_kawai(weights=[r["edge_weight"] for r in edges] )
    #layout = ig.layout_fruchterman_reingold( weights=["{:.2f}".format(r["edge_weight"]) for r in edges],  maxiter=1000, area=N_vertices**3, repulserad=N_vertices**3)
    # TODO ValueError: iterable must yield numbers - Precision del float para el peso del grafo
    layout = ig.layout_fruchterman_reingold( weights=[r["edge_weight"] for r in edges],  maxiter=1000, area=N_vertices**3, repulserad=N_vertices**3)

    # layout = ig.layout_sugiyama(weights=[ r["edge_weight"] for r in edges])
    colors = ["lightgray", "cyan", "magenta", "yellow", "blue", "green", "red"]
    for component in ig.components():
        color = colors[min( 6, len( component ) - 1 )]
        for vidx in component: ig.vs[vidx]["color"] = color

    ig.es["label"] = ig.es.get_attribute_values("weight")
    visual_style["autocurve"] = True
    visual_style["vertex_size"] = 20
    visual_style["vertex_label"] = ig.vs["name"]
    visual_style["vertex_label_size"] = 14
    visual_style["vertex_label_dist"] = 1
    visual_style["vertex_label_angle"] = 1
    visual_style["layout"] = layout
    visual_style["bbox"] = (20 * N_vertices, 20 * N_vertices)  # (600,600)
    visual_style["margin"] = 50
    # visual_style["main"] = "-- igraph plot :"

    return ig, visual_style

def draw_igraph_domain_ip(g):
    #TODO es repetida de la anterior, intentar mejorarla o borrarla
    """
       Version: 1.0
       Function to plot a dispersion of nodes (TupleList) in a graph with igraph
       :param g: GraphFrame
       :return: ig,visual_style : igraph and visual_style to draw the graph
    """
    # from igraph import Graph

    print( "draw_utils draw_igraph --" )

    ig = Graph.TupleList( g.edges.collect(), directed=True )
    visual_style = {}
    N_vertices = ig.vcount()

    layout = ig.layout( "kk" )
    colors = ["lightgray", "cyan", "magenta", "yellow", "blue", "green", "red"]
    for component in ig.components():
        color = colors[min( 6, len( component ) - 1 )]
        for vidx in component: ig.vs[vidx]["color"] = color

    visual_style["vertex_size"] = 20
    visual_style["vertex_label"] = ig.vs["name"]
    visual_style["vertex_label_size"] = 14
    visual_style["vertex_label_dist"] = 1
    visual_style["vertex_label_angle"] = 1
    visual_style["layout"] = layout
    visual_style["bbox"] = (20 * N_vertices, 20 * N_vertices)  # (600,600)
    visual_style["margin"] = 50
    visual_style["main"] = "-- igraph plot :"

    return ig, visual_style


def draw_log_hist(degree, bins=10,path=None):
    '''
    Function to draw a histogram in logaritmic scale.

    :param degree : node degree
    :param bins   : division of the histogram, dos methods
    :param path   : path where to save the histogram image
    :return : ploted bar histogram

    draw_log_hist(degree,10):
    returns 10 bars with division made by np.histogram

    draw_log_hist(degree,[1,10,100,200]):
    returns plot between the numbers passed as a parameter,
    it means that sums the number of elements between 1-10, 10-100,100-200 ....

    LOGARITHMIC SCALE

    '''
    print( "draw_utils draw_log_hist -- --" )

    degree = np.array( degree )
    hist_y, hist_x = np.histogram( degree, bins )
    label_x = [str( int( hist_x[i] ) ) + "-" + str( int( hist_x[i + 1] ) ) for i in range( hist_x.shape[0] - 1 )]

    if f"{path}" is not None:
        plt.savefig( f"{path}" )

    plt.bar( label_x, np.log( hist_y + 1 ) )


def draw_minor_than_list(degree, list_tope,path=None):
    '''

    Function to represent the elements that are minor than a maximum (tope),
    how many are minor than 400, minor than 300, minor than 200 ....
    :param degree   : degree a pintar
    :param list_tope: array of integers with the maximums
    :param path     : path where to save the histogram image
    :return ploted bar histogram
    '''
    print( "draw_utils draw_minor_than_list -- --" )

    degree = np.array( degree )
    count_elemnt = [np.sum( degree <= tope ) for tope in list_tope]
    label_y = [str( t ) for t in list_tope]

    if f"{path}" is not None:
        plt.savefig( f"{path}" )

    plt.bar( label_y, count_elemnt )


def draw_overlap_matrix(df_degree_ratio, list_top_suspicious,figsize=(10,10),path=None):
    '''
    Function to draw an overlap matrix of suspicious domains
    :param df_degree_ratio : dataframe with all the data needed to represent the overlap matrix [a,c,count_ips_in_common,id,outDegree,edge_ratio]
                             where a is src, c is dst, id is src, and outDegree is the outDegree of src. The edge_ratio is calculated with the
                            algorithm proposed.
    :param top_suspicious : number of top suspicious domains to plot
    :param figsize
    :param path : path where to save the histogram image
    :return ploted overlap matrix
    '''
    import matplotlib.ticker as ticker
    matrix_src_dsc = df_degree_ratio.filter(
        (F.col( "a.id" ).isin( list_top_suspicious )) & (F.col( "c.id" ).isin( list_top_suspicious )) ).select(
        F.col( "a.id" ).alias( "src" ), F.col( "c.id" ).alias( "dst" ), F.col( "edge_ratio" ) ).collect()
    dom_idx = dict(
        [(v, k) for k, v in enumerate( list_top_suspicious )] )  # diccionario con indice-dominio y lo invierto
    # para que me lo de dominio-indice de la matriz
    dom_matrix = np.eye(
        len( list_top_suspicious ) )  # matriz con diagonal en 1's de la long de la lista de top_susp
    for s, d, e in matrix_src_dsc:  # relleno la matriz con los valores
        dom_matrix[dom_idx[s], dom_idx[d]] = e
    fig = plt.figure( figsize=figsize )  # tamaño de la matriz
    # fig.suptitle("Overlap domain matriz")
    ax = fig.add_subplot( 111 )
    cax = ax.matshow( dom_matrix )
    fig.colorbar( cax )
    # Set up axes
    # el primero vacio porque si no no pinta el q esta en la posicion 0
    ax.set_xticklabels( [''] + list_top_suspicious, rotation=90 )
    ax.set_yticklabels( [''] + list_top_suspicious )
    ax.xaxis.set_major_locator( ticker.MultipleLocator( 1 ) )
    ax.yaxis.set_major_locator( ticker.MultipleLocator( 1 ) )
    if f"{path}" is not None:
        fig.savefig( f"{path}",format='pdf')

