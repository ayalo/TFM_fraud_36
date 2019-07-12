from pyspark.sql.functions import col

from src.main.python.gf_utils.gf_utils import *
from src.main.python.df_utils.df_utils import *


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
    print( "DomainIpGraph get_vertices-- :" )

    df_dom = df.select( col( "domain_cleaned" ).alias( "id" ) )
    df_ip = df.select( col( "ip_cleaned" ).alias( "id" ) )
    df_vertices = df_dom.union( df_ip )

    return df_vertices


def get_edges(df):
    """
    Creating a df_edges to use GraphFrames
    :param df: dataframe from our data. Idem format like in get_vertices function.
    :return: df_edges
    """
    print( "DomainIpGraph get_edges-- :" )
    #df_edges = df.groupBy( "domain_cleaned", "ip_cleaned" ).count()
    ###... no funciona ...### df_edges = format_edges(df_edges,"domain_cleaned","ip_cleaned","count")
    df_edges = df.groupBy( "domain_cleaned", "ip_cleaned" ).count().select(
        col( "domain_cleaned" ).alias( "src" ), col( "ip_cleaned" ).alias( "dst" ),
        col( "count" ).alias( "edge_weight" ) )  # .persist()

    return df_edges


def get_graph_DI(df,min_edges):
    """
    Get GraphFrame to draw a bipartite graph
    :param df dataframe from our data. Idem format like in get_vertices function.
    :return: gf (GraphFrame graph)

    :definition df_vertices: vertices for the graphframe : domains and ips
    :definition df_edges: links between them
    """
    print( "DomainIpGraph get_graph_DI-- :" )

    df_vertices = get_vertices( df ).persist()
    df_edges = get_edges( df ).persist()

    gf = GraphFrame( df_vertices, df_edges )  # get_graph(df_vertices,df_edges)

    gf_filtered= filter_gf( gf,min_edges)

    return gf_filtered

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
                     '30.50.70.90']} )
    # 'subdomain': ['test1', 'something', 'test2', 'test3', 'else', 'else', 'else', 'else', 'else', 'else']} )
    spark = SparkSession.builder.getOrCreate()
    #df = spark.createDataFrame( data )
    df = spark.read.format( "csv" ).option( "header", 'true' ).option( "delimiter", ',' ).load(
        "/Users/olaya/Documents/Master/TFM/Datos/ssp_bid_compressed_000000000499.csv.gz" )

    print( "DomainIpGraph MAIN-- Pintamos Dataframe completo ..." )
    # df.show()

    print( "DomainIpGraph MAIN-- clean ..." )
    df = clean( df )
    print( "DomainIpGraph MAIN--cleaned df ..." )
    # df.show()
    print( "DomainIpGraph MAIN--get graph DI ... " )
    gf = get_graph_DI( df ,10)

    print( "DomainIpGraph MAIN-- Draw using igraph ..." )
    draw_igraph( gf )
    print( "main -- Draw using nx.Graph :")
    draw_nx(gf.edges)

    return gf


if __name__ == "__main__":
    main()
