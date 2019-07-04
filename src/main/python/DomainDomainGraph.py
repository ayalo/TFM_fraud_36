from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import pandas as pd
from graphframes import *
from graphframes.examples import Graphs
from igraph import *
from pyspark.sql.functions import *

import networkx as nx
import matplotlib.pyplot as plt
from src.main.python.DomainIpGraph import *


def main():
    '''Program entry point'''

    # Intialize a spark context
    # with F.SparkContext( "local", "PySparCreateDataframe" ) as spark:
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

    g= get_graph(df)
    ###g.triplets.show(100,False)

    # Query Graph para obtener interseccion de IPs visitadas por 2 dominios distintos
    df_motifs=g.find( "(a)-[e]->(b); (c)-[e2]->(b)" ).filter("a != c").dropDuplicates(['e','e2'])


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

    df_motifs_count_ips_common = df_motifs.groupBy('a','c').agg(F.collect_list(F.col("b")).alias("count_ips_in_common"))
    rdd_count_motifs = df_motifs_count_ips_common.rdd.map( lambda x: (x.a, x.c, x.count_ips_in_common, len(x.count_ips_in_common)))
    df_motifs_count= rdd_count_motifs.toDF( ["a","c", "count_ips_in_common", "total_ips_in_common"] ) ##· <- Puede ser aqui
    outDeg = g.outDegrees
    print ( "- df_motifs_count : " )
    df_motifs.show(6,False)


    print( "- df_degreeRatio : " )
    df_degree = df_motifs_count.join( outDeg, df_motifs_count.a.id==outDeg.id)
    df_degree.show(10,False)
    print( df_degree.schema )

    df_degree_ratio = df_degree.withColumn( 'edge_ratio', df_degree.total_ips_in_common / df_degree.outDegree )
    print( "- df_degreeRatio division: " )
    df_degree_ratio.show(10,False)
    print( df_degree_ratio.schema )


    #df_edges_DD :src dst edge_ratio
    df_edges_DD_exists=df_degree_ratio.select(df_degree_ratio.a.id, df_degree_ratio.c,
                                              F.when(df_degree_ratio['edge_ratio'] > 0.5,1).otherwise(0))#.show()

    #print( df_edges_DD_exists.show )
    #df_edges_DD_exists.explain() ##BUfF
    print( "df_edges_DD_exists.show" )
    df_edges_DD_exists.show()
    df_edges=df_edges_DD_exists.toDF("src","dst","edge_weight")
    #df_edges = (df_edges_DD_exists
    #            .withColumnRenamed( "a.id", "src" )
    #            .withColumnRenamed( "c", "dst" ))

    print( "df_edges_DD_exists.show---RENAMED" )
    df_edges.show()

    df_vertices_a=df_degree_ratio.select( "a" )
    df_vertices=df_vertices_a.toDF("id")
    #df_vertices = (df_vertices_a
    #            .withColumnRenamed( "a", "id" ))
    print( "df_vertices.show---renamed" )
    df_vertices.show()

    gf = GraphFrame(df_vertices, df_edges)
    draw_nx(df_edges)
    draw_igraph(gf)

if __name__ == "__main__":
    main()