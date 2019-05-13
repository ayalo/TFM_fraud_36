from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import pandas as pd
from graphframes import *
from graphframes.examples import Graphs
from igraph import *

import networkx as nx
import matplotlib.pyplot as plt


def main():
    '''Program entry point'''

    # Intialize a spark context
    # with F.SparkContext( "local", "PySparCreateDataframe" ) as spark:
    data = pd.DataFrame(
            {'domain': ['example.org',
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
                        'website.com'],
             'IP': ['10.20.30.40',
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
                    '10.20.30.42']})
             #'subdomain': ['test1', 'something', 'test2', 'test3', 'else', 'else', 'else', 'else', 'else', 'else']} )
    #sqlContext = SQLContext( spark )
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame( data )
    print (" Pintamos Dataframe completo :")
    df.show()
# Crear funcion Dataframe Domain-IPs
    df_dom = df.select( "domain" )
    df_ip = df.select( "IP" )
    df_vertices = df_dom.union( df_ip )
    print (" Pintamos Dataframe vertices :")
    df_vertices.show()
    # renombramos columna 'domain' para que graphframes encuentre columna 'id' y pueda crear el grafo.
    df_vertices = (df_vertices
                .withColumnRenamed( "domain", "id" ))
    print( "- df_vertices.explain()" )
    df_vertices.explain()

    ######@ No se necesita añadir el indice puesto que podemos renombrar la columna para que utilice el propio 'domain' como 'id'
    ### Añadimos el indice
    #df_vertices.rdd.zipWithIndex()
    ### Ahora tenemos un rdd que tiene en la primera posicion el row y en segunda el index, transformar en la estructura
    ## que necesito

    # genero tupla correcta
    #df_vertices.rdd.zipWithIndex().map( lambda x: (x[0].domain, x[1]) )
    # Cojo de la tupla que tengo el primer elemento y cojo la columna domain y cojo el segundo elemento que es el indice
    # Ahora ya lo puedo transformar a un dataframe de nuevo

    ##df_vertices_index = df_vertices.rdd.zipWithIndex().map( lambda x: (x[1], x[0].domain) ).toDF( ["id", "nodos"] )
    ##type( df_vertices_index )
    ##print (" Pintamos Dataframe indices - vertices :")
    ##df_vertices_index.show()
    ######@

    ##EN EL SIGUIENTE CREO QUE SI TUVIERA MAS CAMPOS EL DF, AL HACER EL GROUPBY LOS ELIMINO, PREG LUIS
    df_count_domain_ips = df.select("domain","IP").groupBy("domain").agg(F.collect_list(F.col("IP")).alias("IP_list"))
    print( " Pintamos DF df_count_domain_ips.show :" )
    df_count_domain_ips.show()
    rdd_count_domain_ips = df_count_domain_ips.rdd.map(lambda x:(x.domain,x.IP_list,len(x.IP_list)))
    df_count_domain_ips=rdd_count_domain_ips.toDF(["domain", "IP_list", "total_links"]) #--> Tabla domain-ip-total_visitas

    print( " Pintamos DF df_count_domain_ips.show :" )
    df_count_domain_ips.show(5,False)
    df_edges=df.groupBy("domain","IP").count()
    print( " Pintamos DF df_edges.show :" )
    df_edges.show()

    df_edges= (df_edges
            .withColumnRenamed("domain","src")
            .withColumnRenamed("IP","dst"))
    print( "- df_edges.explain()")
    df_edges.explain()

    print( " Pintamos DF edges.show :" )
    df_edges.show()
## Generar funcion Crea GraphFrame
    print("Creamos GraphFrame -- ")
    ##g = GraphFrame(df_vertices_index, df_edges) # de cuando añadiamos columna 'id'  y no renombrabamos 'domain'
    g = GraphFrame(df_vertices, df_edges)
    print("Pasa de GrapFrame- Pintamos g.vertices : ")
    g.vertices.show()
    print("- Pintamos g.edges : ")
    g.edges.show()
    print ("Check the number of edges of each vertex")
    g.degrees.show()
    print ("Check the in-degrees")
    inDeg = g.inDegrees
    inDeg.orderBy(F.desc( "inDegree" )).show(5,False)
    print("Check thte out-degrees")
    outDeg = g.outDegrees
    outDeg.orderBy(F.desc( "outDegree" ) ).show(5,False)

    print ("Show only connected components")
    spark.sparkContext.setCheckpointDir( 'prueba_graphframes_cps' )
    g.connectedComponents().show()

    # Query Graph para obtener interseccion de IPs visitadas por 2 dominios distintos
    df_motifs=g.find( "(a)-[]->(b); (c)-[]->(b)" ).filter("a != c").dropDuplicates(['b'])
    df_motifs.show(300, False)

    df_motifs_count_ips_common = df_motifs.groupBy('a','c').agg(F.collect_list(F.col("b")).alias("count_ips_in_common"))
    print("- motifs_count : ")
    df_motifs_count_ips_common.show(4,False)

    rdd_count_motifs = df_motifs_count_ips_common.rdd.map( lambda x: (x.a, x.c, x.count_ips_in_common, len(x.count_ips_in_common)) )
    df_motifs_count= rdd_count_motifs.toDF( ["id","c", "count_ips_in_common", "total_ips_in_common"] )
    df_motifs_count.show()



    print( "- df_degreeRatio : " )
    df_degreeRatio = df_motifs_count.join( outDeg, df_motifs_count.select(id) = outDeg.select('id'))
        #.selectExpr( "id","double(total_ips_in_common)/double(outDegree) as degreeRatio" )
    #df_degreeRatio.orderBy( F.desc( "degreeRatio" ) ).show( 10, False )
    df_degreeRatio.show(10,False)


    ## Crear funcion Pinta GrapFrame
    print("- Pintamos Grafo- TupleList : ")

    #igraph=Graph.TupleList(g.edges.collect(), directed=True)
    #plot(igraph)

#https://igraph.org/python/doc/igraph-pysrc.html#Graph.Bipartite
    #g2 = Graph.Bipartite( [0, 1, 0, 1], [(0, 1), (2, 3), (0, 3)] )
    #g2.is_bipartite()
    #g2.vs["type"]
    #plot(g2)

# https://stackoverflow.com/questions/30850688/construct-bipartite-graph-from-columns-of-python-dataframe

    B = nx.Graph()
    B.add_nodes_from(data['domain'], bipartite=1)
    B.add_nodes_from(data['IP'], bipartite=0)
    B.add_weighted_edges_from(
        [(row['domain'], row['IP'], 1) for idx, row in data.iterrows()],
        weight='weight')

    print(B.edges(data=True))
    # [('test1', 'example.org', {'weight': 1}), ('test3', 'example.org', {'weight': 1}), ('test2', 'example.org', {'weight': 1}), ('website.com', 'else', {'weight': 1}), ('site.com', 'something', {'weight': 1})]

    pos = {node:[0, i] for i,node in enumerate(data['domain'])}
    pos.update({node:[1, i] for i,node in enumerate(data['IP'])})
    nx.draw(B, pos, with_labels=False)
    for p in pos:  # raise text positions
        pos[p][1] += 0.25
    nx.draw_networkx_labels(B, pos)

    plt.show()




if __name__ == "__main__":
    main()