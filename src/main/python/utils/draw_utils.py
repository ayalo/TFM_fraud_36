import matplotlib.pyplot as plt
from igraph import Graph
from igraph import plot
import networkx as nx
import pandas as pd
import numpy as np
import matplotlib.ticker as ticker
from pyspark.sql import functions as F
from pyspark.sql.functions import col



def draw_nx(df_edges):  # usada en DI y DD, no funciona con muchos datos
    """
    Function to plot a bipartite graph with networkx
    :param df_edges: df_edges from a GraphFrame
    :return: ploted graph
    """

    #import networkx as nx
    #import pandas as pd

    print( "draw_utils draw_nx --" )

    df = df_edges.toPandas()  ##GUARRADA

    B = nx.Graph()
    print( "draw_utils draw_nx -- -- despues nx.Graph()" )

    B.add_nodes_from( df['src'], bipartite=1 )
    print( "draw_utils draw_nx -- -- despues add_nodes_from src" )

    B.add_nodes_from( df['dst'], bipartite=0 )
    print( "draw_utils draw_nx -- -- despues add_nodes_from dst" )
    #B.add_weighted_edges_from(
    #    [(row['src'], row['dst'], 1) for idx, row in df.iterrows()],
    #    weight=row[] )

    B.add_edges_from( zip( df['src'], df['dst'] ), weight=1 )

    print( "draw_utils draw_nx -- -- Nodes added to B" )

    # print( B.edges( data=True ) )
    # [('test1', 'example.org', {'weight': 1}), ('test3', 'example.org', {'weight': 1}), ('test2', 'example.org', {'weight': 1}),
    # ('website.com', 'else', {'weight': 1}), ('site.com', 'something', {'weight': 1})]

    pos = {node: [0, i] for i, node in enumerate( df['src'] )}
    pos.update( {node: [1, i] for i, node in enumerate( df['dst'] )} )
    print( "draw_utils draw_nx -- -- despues de pos.update" )
    nx.draw( B, pos, with_labels=False )
    for p in pos:  # raise text positions
        pos[p][1] += 0.10
    print( "draw_utils draw_nx -- -- despues for " )
    nx.draw_networkx_labels( B, pos )
    print( "draw_utils draw_nx -- -- ante de plot" )
    plt.show()

# TODO borrar la siguiente :
'''
def draw_igraph(g):  ##usada en DI y DD TODO : borrarla 
    """
    Version: 1.0
    Function to plot a dispersion of nodes (TupleList) in a graph with igraph
    :param g: GraphFrame
    :return: ploted graph
    """

    from igraph import Graph
    from igraph import plot


    print( "gf_utils draw_igraph --" )

    ig2 = Graph.TupleList( g.edges.collect(), directed=True )
    # ig2 = Graph.Erdos_Renyi(n=300, m=250)
    visual_style = {}
    N_vertices = ig2.vcount()

    layout = ig2.layout( "kk" )
    # layout = ig.layout("fr")
    # layout = layout.fruchterman.reingold
    colors = ["lightgray", "cyan", "magenta", "yellow", "blue", "green", "red"]
    for component in ig2.components():
        color = colors[min( 6, len( component ) - 1 )]
        for vidx in component: ig2.vs[vidx]["color"] = color

    visual_style["vertex_size"] = 20
    visual_style["vertex_label"] = ig2.vs["name"]
    visual_style["vertex_label_size"] = 14  # tamaño de la letra (hay q ver como se cambia el tipo de letra)
    visual_style["vertex_label_dist"] = 1  # coloco etiqueta debajo del nodo
    visual_style["vertex_label_angle"] = 1  # coloco etiqueta a la derecha: 0
    # visual_style["edge_width"] = [7] * (N_vertices - 1)
    # visual_style["edge_width"] = [1 + 2 * int(is_formal) for is_formal in ig.es["is_formal"]]
    visual_style["layout"] = layout
    visual_style["bbox"] = (20 * N_vertices, 20 * N_vertices)  # (600,600)
    # visual_style["bbox"] = (300, 300)
    visual_style["margin"] = 50
    visual_style["main"] = "-- igraph plot :"
    plot( ig2, **visual_style )
'''
def draw_igraph_domain_domain(g_domdom):  ##usada en DI y DD
    """
    Version: 1.0
    Function to plot a dispersion of nodes (TupleList) in a graph with igraph
    :param g: GraphFrame
    :return: ploted graph
    """
    #from igraph import Graph

    print( "draw_utils draw_igraph --" )
    edges=g_domdom.edges.select( F.col( "src.id" ).alias( "src" ), F.col( "dst.id" ).alias( "dst" ),
                            F.col( "edge_weight" ) ).collect()

    ig = Graph.TupleList( edges, directed=True )
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
    #visual_style["main"] = "-- igraph plot :"

    return ig, visual_style

def draw_igraph_domain_ip(g):
    #TODO es repetida de la anterior, intentar mejorarla o borrarla
    """
       Version: 1.0
       Function to plot a dispersion of nodes (TupleList) in a graph with igraph
       :param g: GraphFrame
       :return: ploted graph
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
    degree : node degree
    bins: division del histograma, dos formas

    draw_log_hist(degree,10):
    devuelve 10 barras con una division realizada por el np.histogram

    draw_log_hist(degree,[1,10,100,200]):

    devuelve plot entre los numeros que le pases,
    es decir suma el numero de elementos que hay entre 1-10 , 10-100,100-200 ....

    ESCALA LOGARITMICA

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

    degree : degree a pintar
    list_tope: array de enteros con los topes
    funcion que representa los elementos que hay menores que un tope,
    cuantos hay menores que 400, menores que 300 , menores que 200 ....

    '''
    print( "draw_utils draw_minor_than_list -- --" )

    degree = np.array( degree )
    count_elemnt = [np.sum( degree <= tope ) for tope in list_tope]
    label_y = [str( t ) for t in list_tope]

    if f"{path}" is not None:
        plt.savefig( f"{path}" )

    plt.bar( label_y, count_elemnt )


def draw_overlap_matrix(df_degree_ratio, list_top_suspicious,path=None):
    '''
    df_degree_ratio
    top_suspicious : number of top suspicious domains to plot

    '''
    print( "draw_utils draw_overlap_matrix -- --" )

    matrix_src_dsc = df_degree_ratio.filter(
        (F.col( "a.id" ).isin( list_top_suspicious )) & (F.col( "c.id" ).isin( list_top_suspicious )) ).select(
        F.col( "a.id" ).alias( "src" ), F.col( "c.id" ).alias( "dst" ), F.col( "edge_ratio" ) ).collect()

    dom_idx = dict( [(v, k) for k, v in enumerate( list_top_suspicious )] )# diccionario con indice-dominio y lo invierto
    # para que me lo de dominio-indice de la matriz

    dom_matrix = np.eye( len( list_top_suspicious ) )

    for s, d, e in matrix_src_dsc:
        dom_matrix[dom_idx[s], dom_idx[d]] = e

    fig = plt.figure()

    # fig.suptitle("Overlap domain matriz")
    ax = fig.add_subplot( 111 )
    cax = ax.matshow( dom_matrix )
    fig.colorbar( cax )

    # Set up axes
    ax.set_xticklabels( [''] + list_top_suspicious, rotation=90 )
    ax.set_yticklabels( [''] + list_top_suspicious )

    ax.xaxis.set_major_locator( ticker.MultipleLocator( 1 ) )
    ax.yaxis.set_major_locator( ticker.MultipleLocator( 1 ) )
    if f"{path}" is not None:
        fig.savefig( f"{path}",format='pdf')

