
import pandas as pd

from utils.gf_utils import *
from utils.df_utils import *
from utils.draw_utils import *
from utils.spark_utils import *

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
                             'example.org',
                             'website.com'],
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
                     '30.50.70.90',
                     '10.20.30.40']} )
    # 'subdomain': ['test1', 'something', 'test2', 'test3', 'else', 'else', 'else', 'else', 'else', 'else']} )
    spark = spark_session()
    # df = spark.createDataFrame( data )
    df = spark.read.format( "csv" ).option( "header", 'true' ).option( "delimiter", ',' ).load(
        "/Users/olaya/Documents/Master/TFM/Datos/ssp_bid_compressed_000000000499.csv.gz" )

    print( "DomainDomainGraph MAIN -- Pintamos Dataframe completo:" )
    # df.show()
    # print_show( df )

    df = clean( df,"referrer_domain","user_ip")

    # g= src.main.python.DomainIpGraph.get_graph(df)
    g_domip = get_graph_domip( df, 15 ).persist()
    print( "DomainDomainGraph MAIN -- triplets " )
    # g_domip.triplets.show( 100, False )

    gf = get_graph_domdom( g_domip ).persist()

    draw_igraph( gf )


'''
    #TODO:: mover a un notebook

    gf.edges.write.parquet("saved_graph/edges_prueba")

    gf.vertices.write.parquet("saved_graph/vert_prueba")

    draw_igraph( gf )

    # TODO:: mover a una clase solo

    ed_2 = spark.read.parquet("saved_graph/edges_prueba")

    ver_2 = spark.read.parquet("saved_graph/vert_prueba")


    gf_2 =  GraphFrame(ver_2,ed_2)

    draw_igraph( gf_2 )
'''
# draw_nx( gf.edges )

# print( "DomainDomainGraph MAIN -- Show only connected components" )
# spark.sparkContext.setCheckpointDir( 'DomainDomainGraph_cps' )
## components=gf.connectedComponents().sort()# ## necesita una carpeta para cps
# components = gf.stronglyConnectedComponents( maxIter=10 ).select( "id", "component" ).groupBy( "component" ).agg(
#    F.count( "id" ).alias( "count" ) ).orderBy( F.desc( "count" ) )
# print( "DomainDomainGraph MAIN -- show components" )
# components.show()

if __name__ == "__main__":
    main()
