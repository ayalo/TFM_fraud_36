#saca edges
#saca vertices
#pinta grafo
# intentar hacer las funciones mas genericas para eliminar las referencias a referrer_domain y user_ip y sacar una clase DrawGraph generica
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import pandas as pd
from graphframes import *

from igraph import *
import networkx as nx
import matplotlib.pyplot as plt

from src.main.python.DomainCleaner import domain_cleaner
from src.main.python.IpCleaner import ip_cleaner

from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import *


def get_vertices(df):
    """
    Creating a df_vertices to use GrapFrames
    :param df:  dataframe from our data.
                format:
                    root
                        |-- user_ip: string (nullable = true)
                        |-- uuid_hashed: string (nullable = true)
                        |-- useragent: string (nullable = true)
                        |-- referrer_domain: string (nullable = true)
                        |-- ssp_domain: string (nullable = true)
                        |-- date_time: string (nullable = true)
    :return: df_vertices

    """
    df_dom = df.select( "referrer_domain" )
    df_ip = df.select( "user_ip" )
    df_vertices = df_dom.union( df_ip )
    #print (" Pintamos Dataframe vertices :")
    #df_vertices.show()
    # renombramos columna 'referrer_domain' para que graphframes encuentre columna 'id' y pueda crear el grafo.
    df_vertices=df_vertices.toDF("id")
    #df_vertices = (df_vertices
    #            .withColumnRenamed( "referrer_domain", "id" ))
    #print( "- df_vertices.explain()" )

    return df_vertices



def get_edges(df):
    """
    Creating a df_edges to use GraphFrames
    :param df: dataframe from our data. Idem format like in get_vertices function.
    :return: df_edges
    """
    df_count_domain_ips = df.select("referrer_domain","user_ip").groupBy("referrer_domain").agg(F.collect_list(F.col("user_ip")).alias("IP_list"))
    #print( " Pintamos DF df_count_domain_ips.show :" )
    #df_count_domain_ips.show()
    rdd_count_domain_ips = df_count_domain_ips.rdd.map(lambda x:(x.referrer_domain,x.IP_list,len(x.IP_list)))
    df_count_domain_ips=rdd_count_domain_ips.toDF(["referrer_domain", "IP_list", "total_links"]) #--> Tabla domain-ip-total_visitas

    #print( " Pintamos DF df_count_domain_ips.show :" )
    #df_count_domain_ips.show(5,False)
    df_edges=df.groupBy("referrer_domain","user_ip").count()
    #print( " Pintamos DF df_edges.show :" )
    #df_edges.show()

    df_edges=df_edges.toDF("src","dst","edge_weight")
    #df_edges= (df_edges
    #        .withColumnRenamed("referrer_domain","src")
    #        .withColumnRenamed("user_ip","dst"))
    #print( "- df_edges.explain()")
    #df_edges.explain()

    #print( " Pintamos DF edges.show :" )
    #df_edges.show()

    return df_edges

def get_graph_DI(df):
    """
    Get GraphFrame to draw a bipartite graph
    :param df dataframe from our data. Idem format like in get_vertices function.
    :return: gf (GraphFrame graph)

    :definition df_vertices: vertices for the graphframe : domains and ips
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

def clean(df):
    df_dropna = df.dropna( subset=('user_ip', 'referrer_domain') )
    #df_dropna.head()

    # Añado una columna mas al df_current con el user_ip normalizado : ip_cleaned
    # Añado una columna mas al df_current con el referrer_domain normalizado : domain_cleaned

    udf_ipcleaner = udf( ip_cleaner, StringType() )
    udf_domaincleaner = udf( domain_cleaner, StringType() )

    print( "Calculando df_cleaned_ip" )
    df_cleaned_ip = df_dropna.withColumn( 'ip_cleaned', udf_ipcleaner( df.user_ip ) )
    df_cleaned_ip.select( "referrer_domain" ).count()
    #df_cleaned_ip.head()

    df_cleaned = df_cleaned_ip.withColumn( 'domain_cleaned', udf_domaincleaner( df_cleaned_ip.referrer_domain ) )
    df_cleaned.select( "referrer_domain" ).count()
    #df_cleaned.head()

    df_cleaned_dropna = df_cleaned.dropna( subset=('user_ip', 'referrer_domain', 'ip_cleaned', 'domain_cleaned') )

    ## DROP/Filter Format not valid in ip_cleaned
    df_cleaned_format = df_cleaned_dropna.filter( (df.ip_cleaned != 'Format not valid') )


    return df_cleaned_format

def main():
    '''Program entry point

    :return gf : graph graphframe Domain-IP bipartite relation
    '''

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
    ####df = spark.createDataFrame( data )
    df = spark.read.format("csv").option("header", 'true').option("delimiter", ',').load("/Users/olaya/Documents/Master/TFM/Datos/ssp_bid_compressed_000000000499.csv.gz")


    print (" Pintamos Dataframe completo :")
    df.show()

    df=clean(df)

    print("cleaned df :")
    df.show()

    print ("get graph DI : ")
    gf=get_graph_DI( df )G
    print( "MAIN -- gf -- Check the number of edges of each vertex" )
    gf.degrees.show()


    print( "main -- Draw using igraph :")
    draw_igraph(gf)
    print( "main -- Draw using nx.Graph :")
    draw_nx(get_edges(df))

    return gf

if __name__ == "__main__":
    main()