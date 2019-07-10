from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import pandas as pd
from graphframes import *

from igraph import *
import networkx as nx
import matplotlib.pyplot as plt

from src.main.python.DomainIpGraph import get_graph_DI

def get_graph(df):
    """
    Get GraphFrame to draw a bipartite graph
    :param df dataframe from our data. Idem format like in get_vertices function.
    :return: gf (GraphFrame graph)

    :definition df_vertices: vertices for the graphframe : domains
    :definition df_edges: links between them
    """

    df_vertices=get_vertices(df)
    df_edges=get_edges(df)

    print("MAIN -- df_vertices: --")
    df_vertices.show()
    print("MAIN -- df_edges: --")
    df_edges.show()

    ## Generar funcion Crea GraphFrame
    print("Creamos GraphFrame -- ")
    ##g = GraphFrame(df_vertices_index, df_edges) # de cuando añadiamos columna 'id'  y no renombrabamos 'domain'
    gf = GraphFrame(df_vertices, df_edges)

    return gf

def draw_igraph(g):
    """
    :param g:
    :return:
    """
    ig = Graph.TupleList( g.edges.collect(), directed=True )
    plot( ig )


def draw_nx (df_edges):
    """

    :param df_edges: df_edges from a GraphFrame
    :return:
    """

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

def get_vertices(df):
    """
    Creating a df_vertices to use GrapFrames
    :param df:
    :return: df_vertices

    """
    df_vertices=df.select( "a" ).toDF("id")
    print( "df_vertices.show --- renamed" )
    df_vertices.show()

    return df_vertices

def get_edges(df):
    """
    Creating a df_edges to use GraphFrames
    :param df: dataframe from our data. Idem format like in get_vertices function.
    :return: df_edges
    """

    df_edges_DD_exists = df.select( df.a.id, df.c,
                                                 F.when( df['edge_ratio'] > 0.5, 1 ).otherwise( 0 ) )  # .show()
    df_edges=df_edges_DD_exists.toDF("src","dst","edge_weight")

    print( "df_edges.show -- renamed" )
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
                        'example.org'],
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
                    '30.50.70.90']})
             #'subdomain': ['test1', 'something', 'test2', 'test3', 'else', 'else', 'else', 'else', 'else', 'else']} )
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame( data )
    print (" Pintamos Dataframe completo :")
    df.show()

    #g= src.main.python.DomainIpGraph.get_graph(df)
    g=get_graph_DI(df)
    ###g.triplets.show(100,False)

    # Query  DomainIpGraph para obtener interseccion de IPs visitadas por 2 dominios distintos
    df_motifs=g.find( "(a)-[e]->(b); (c)-[e2]->(b)" ).filter("a != c").dropDuplicates(['e','e2'])

    df_motifs_count = df_motifs.groupBy( 'a', 'c' ).agg(F.count(F.col("b")).alias("count_ips_in_common"))

    df_motifs_count.show()

#....................... NO VA PERO PARECE MEJOR
#    df_motifs_count_ips_common= df_motifs.groupBy('a','c').count()
#    df_motifs_count_ips_common.show()
#    ## df_motifs_count_ips_common = df_motifs.join(df_aux_count, "a")##.dropDuplicates()
#    df_motifs_count_ips_common.show()
#    print( "- motifs_count : " )
#    df_motifs_count_ips_common.show(6,False)
#    print( df_motifs_count_ips_common.schema )
#    #print ("df_motifs_count_ips_common describe")
#    #df_motifs_count_ips_common.describe().show()
#    outDeg = g.outDegrees
#
#    df_degree = df_motifs_count_ips_common.join( outDeg, df_motifs_count_ips_common.a.id == outDeg.id )
#    print( "--> df_degree ( ips in common - outDeg ) : --" )
#    df_degree.show()
#    print( df_degree.schema )
#    #print ("df_degree describe")
#    #df_degree.describe().show()
#
#    print ("df_degree.count : ")
#    df_degree_ratio = df_degree.withColumn( 'edge_ratio', df_degree.count /df_degree.outDegree)
#    print( "- df_degreeRatio division: " )
#    df_degree_ratio.show( 10, False )
#.FIN...................... NO VA PERO PARECE MEJOR

###    df_motifs_count_ips_common = df_motifs.groupBy('a','c').agg(F.collect_list(F.col("b")).alias("count_ips_in_common"))
###    rdd_count_motifs = df_motifs_count_ips_common.rdd.map( lambda x: (x.a, x.c, x.count_ips_in_common, len(x.count_ips_in_common)))
###    df_motifs_count= rdd_count_motifs.toDF( ["a","c", "count_ips_in_common", "total_ips_in_common"] ) ##· <- Puede ser aqui
    outDeg = g.outDegrees
    print ( "- df_motifs_count : " )
    df_motifs.show(6,False)


    print( "- df_degreeRatio : " )
    df_degree = df_motifs_count.join( outDeg, df_motifs_count.a.id==outDeg.id)
    df_degree.show(10,False)
    print( df_degree.schema )

    df_degree_ratio = df_degree.withColumn( 'edge_ratio', df_degree.count_ips_in_common / df_degree.outDegree )
    print( "- df_degreeRatio division: " )
    df_degree_ratio.show(10,False)
    print( df_degree_ratio.schema )


    #df_edges_DD :src dst edge_ratio
    #df_edges=get_edges(df_degree_ratio)
    #df_edges_DD_exists=df_degree_ratio.select(df_degree_ratio.a.id, df_degree_ratio.c,
    #                                          F.when(df_degree_ratio['edge_ratio'] > 0.5,1).otherwise(0))#.show()

    #print( df_edges_DD_exists.show )
    #df_edges_DD_exists.explain() ##BUfF
    #print( "df_edges_DD_exists.show" )
    #df_edges_DD_exists.show()

    #df_edges=df_edges_DD_exists.toDF("src","dst","edge_weight")
    #df_edges = (df_edges_DD_exists
    #            .withColumnRenamed( "a.id", "src" )
    #            .withColumnRenamed( "c", "dst" ))

    #print( "df_edges_DD_exists.show---RENAMED" )
    #df_edges.show()

    #df_vertices=get_vertices(df_degree_ratio.select( "a" ))
    # df_vertices_a=df_degree_ratio.select( "a" )
    #df_vertices = (df_vertices_a
    #            .withColumnRenamed( "a", "id" ))
    #print( "df_vertices.show---renamed" )
    #df_vertices.show()

    gf = get_graph(df_degree_ratio)
    draw_igraph(gf)
    draw_nx(get_edges(df_degree_ratio))

if __name__ == "__main__":
    main()

## Preguntar :
# generar clases DomainDomainGraph y DomainIpGraph para poder compartir metodo que se llame igual get_graph pero llamar desde la primera a
# el metodo de la segunda clase.

# parte comentada, como renombro la columna count, si no tiene el mismo formato (no es String en un longbyte) para poder meterlo en edges.
# creo que esa parte es mas eficiente puesto que no necesito tener la lista de ips visitadas por dominio, solo saber cuantas.
