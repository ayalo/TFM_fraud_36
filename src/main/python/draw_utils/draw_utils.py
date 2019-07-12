import pandas as pd
from graphframes import *



import matplotlib.pyplot as plt




def draw_nx(df_edges):  # usada en DI y DD, no funciona con muchos datos
    """
    Function to plot a bipartite graph with networkx
    :param df_edges: df_edges from a GraphFrame
    :return: ploted graph
    """

    import networkx as nx

    print( "df_utils draw_nx --" )

    df = df_edges.toPandas()  ##GUARRADA

    B = nx.Graph()
    print( "df_utils draw_nx -- -- despues nx.Graph()" )

    B.add_nodes_from( df['src'], bipartite=1 )
    print( "df_utils draw_nx -- -- despues add_nodes_from src" )

    B.add_nodes_from( df['dst'], bipartite=0 )
    print( "df_utils draw_nx -- -- despues add_nodes_from dst" )
    #B.add_weighted_edges_from(
    #    [(row['src'], row['dst'], 1) for idx, row in df.iterrows()],
    #    weight='weight' )

    B.add_edges_from(zip(df['src'],df['dst']),weight=1)

    print( "df_utils draw_nx -- -- Nodes added to B" )

    #print( B.edges( data=True ) )
    # [('test1', 'example.org', {'weight': 1}), ('test3', 'example.org', {'weight': 1}), ('test2', 'example.org', {'weight': 1}),
    # ('website.com', 'else', {'weight': 1}), ('site.com', 'something', {'weight': 1})]

    pos = {node: [0, i] for i, node in enumerate( df['src'] )}
    pos.update( {node: [1, i] for i, node in enumerate( df['dst'] )} )
    print("df_utils draw_nx -- -- despues de pos.update")
    nx.draw( B, pos, with_labels=False )
    for p in pos:  # raise text positions
        pos[p][1] += 0.10
    print("df_utils draw_nx -- -- despues for ")
    nx.draw_networkx_labels( B, pos )
    print("df_utils draw_nx -- -- ante de plot")
    plt.show()



def draw_igraph(g):  ##usada en DI y DD
    """
    Function to plot a dispersion of nodes (TupleList) in a graph with igraph
    :param g: GraphFrame
    :return: ploted graph
    """

    from igraph import Graph
    from igraph import plot

    print( "gf_utils draw_igraph --" )

    ig = Graph.TupleList( g.edges.collect(), directed=True )
    print( "gf_utils draw_igraph ---- despues ig ---" )
    plot( ig )
