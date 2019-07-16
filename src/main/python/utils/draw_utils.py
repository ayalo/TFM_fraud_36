import matplotlib.pyplot as plt


def draw_nx(df_edges):  # usada en DI y DD, no funciona con muchos datos
    """
    Function to plot a bipartite graph with networkx
    :param df_edges: df_edges from a GraphFrame
    :return: ploted graph
    """

    import networkx as nx
    import pandas as pd

    print( "df_utils draw_nx --" )

    df = df_edges.toPandas()  ##GUARRADA

    B = nx.Graph()
    print( "df_utils draw_nx -- -- despues nx.Graph()" )

    B.add_nodes_from( df['src'], bipartite=1 )
    print( "df_utils draw_nx -- -- despues add_nodes_from src" )

    B.add_nodes_from( df['dst'], bipartite=0 )
    print( "df_utils draw_nx -- -- despues add_nodes_from dst" )
    # B.add_weighted_edges_from(
    #    [(row['src'], row['dst'], 1) for idx, row in df.iterrows()],
    #    weight='weight' )

    B.add_edges_from( zip( df['src'], df['dst'] ), weight=1 )

    print( "df_utils draw_nx -- -- Nodes added to B" )

    # print( B.edges( data=True ) )
    # [('test1', 'example.org', {'weight': 1}), ('test3', 'example.org', {'weight': 1}), ('test2', 'example.org', {'weight': 1}),
    # ('website.com', 'else', {'weight': 1}), ('site.com', 'something', {'weight': 1})]

    pos = {node: [0, i] for i, node in enumerate( df['src'] )}
    pos.update( {node: [1, i] for i, node in enumerate( df['dst'] )} )
    print( "df_utils draw_nx -- -- despues de pos.update" )
    nx.draw( B, pos, with_labels=False )
    for p in pos:  # raise text positions
        pos[p][1] += 0.10
    print( "df_utils draw_nx -- -- despues for " )
    nx.draw_networkx_labels( B, pos )
    print( "df_utils draw_nx -- -- ante de plot" )
    plt.show()


def draw_igraph_origin(g):  ##usada en DI y DD
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

def draw_igraph(g):  ##usada en DI y DD
    """
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
def draw_igraph_bipartite(g):  ##usada en DI y DD
    """
    Function to plot a dispersion of nodes (TupleList) in a graph with igraph
    :param g: GraphFrame
    :return: ploted graph
    """
    from igraph import Graph
    from igraph import plot

    print( "gf_utils draw_igraph_bipartite --" )
    igb = Graph.Bipartite( ,g.edges.collect(), directed=False )
    igb.vs["type"]=0
    igb.vs[n1:]["type"]=1

    plot( igb)
'''