from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import *

from utils.gf_utils import *

from utils.row_cleaners_utils import domain_cleaner
from utils.row_cleaners_utils import ip_cleaner


def is_string(s):
    """
    Function that returns true if s is a string or false if not
    The intention of this function is filter all the types into the df that have no normalized type in 'referrer_domain'
    or 'user_ip' columns, in order to clean the original dataset.
    :param s: string passed as an argument
    :return: boolean True/False
    """
    # return isinstance( s, basestring )
    return type( s ) is str


def clean(df, referrer_domain, user_ip):  # usada en DI (con el df original) y DD (con el df original)
    """
    Function to clean the original data from df, eliminate or filter null, none, and other types that are not string in
    'referrer_domain' or 'user_ip' columns, in order to clean the original dataset.
    :param df:
    :param referrer_domain: first column to clean
    :param user_ip: second column to clean
    :return: df_cleaned_format
    """

    print( "df_utils clean --" )

    udf_filter_string = udf( is_string, BooleanType() )

    df = df.filter(
        (udf_filter_string( F.col( f"{referrer_domain}" ) )) & (udf_filter_string( F.col( f"{user_ip}" ) )) )

    # Adding new column to df with the normalized/filtered/cleaned user_ip : ip_cleaned
    udf_ipcleaner = udf( ip_cleaner, StringType() )
    # print( "df-utils clean-- Calculando df_cleaned_ip" )
    df_cleaned_ip = df.withColumn( 'ip_cleaned', udf_ipcleaner( df.user_ip ) )

    # Adding new column to df with the normalized/filtered/cleaned referrer_domain : domain_cleaned
    udf_domaincleaner = udf( domain_cleaner, StringType() )
    df_cleaned = df_cleaned_ip.withColumn( 'domain_cleaned', udf_domaincleaner( df_cleaned_ip.referrer_domain ) )

    ## DROP/Filter Format not valid in ip_cleaned
    df_cleaned_format = df_cleaned.filter( (df_cleaned.ip_cleaned != 'Format not valid') )

    return df_cleaned_format


def get_vertices(df_edges, a, b):
    """
    Function to Rename the columns of  df_vertices in order to use GraphFrames [id].
    This function renames the columns a and b with the alias id, and returns the dataframe df_vertices,
    with a single column and with dropedDuplicates.
    :param df_edges: dataframe of edges with format for a GraphFrames use (src,dst,edge_weight)
    :param a: df column
    :param b: df column
    :return:
    """
    print( "df_utils get_vertices-- :" )

    df_dom = df_edges.select( col( f"{a}" ).alias( "id" ) )
    df_ip = df_edges.select( col( f"{b}" ).alias( "id" ) )

    # print( "df_utils get_vertices-- df_vertices_withduplicates :" )
    df_vertices_withduplicates = df_dom.union( df_ip )
    # df_vertices_withduplicates.show()

    # print( "df_utils get_vertices-- df_vertices_sin duplicates :" )
    df_vertices = df_vertices_withduplicates.dropDuplicates()
    # df_vertices.show()

    return df_vertices


def get_edges(df, a, b, c):
    '''
    Function to Rename the columns of df_edges in the correct format to GraphFrames [src, dst, edge_weight]
    :param df: df_edges with the incorrect name columns
    :param a: old name for column src
    :param b: old name for column dst
    :param c: old name for column edge_weight
    :return: df_edges
    '''
    print( "df_utils get_edges --" )
    df_edges = df.select(
        col( f"{a}" ).alias( "src" ), col( f"{b}" ).alias( "dst" ),
        col( f"{c}" ).alias( "edge_weight" ) )

    return df_edges


def get_edges_domip(df):
    """
    Creating a df_edges to use GraphFrames in order to create a domain-ip graph.
    :param df: dataframe from our data. Idem format like in get_vertices function.
                format:
                    root
                        |-- user_ip: string (nullable = true)
                        |-- uuid_hashed: string (nullable = true)
                        |-- useragent: string (nullable = true)
                        |-- referrer_domain: string (nullable = true)
                        |-- ssp_domain: string (nullable = true)
                        |-- date_time: string (nullable = true)
    :return: df_edges
    """
    print( "df_utils get_edges_domip-- : df" )
    # df.show()
    df_edges_count = df.groupBy( "domain_cleaned", "ip_cleaned" ).count()
    # print( "df_utils get_edges_domip-- :df_edges_count" )
    # df_edges_count.show()
    df_edges = get_edges( df_edges_count, "domain_cleaned", "ip_cleaned", "count" )
    # print( "df_utils get_edges_domip-- :df_edges" )
    # df_edges.show()

    return df_edges


def get_edges_domdom(df):
    """
    Creating a df_edges to use GraphFrames in order to create a domain-domain graph.
    :param df: dataframe from our data. Idem format like in get_vertices function. df_degree_ratio
    :return: df_edges
    """
    print( "df_utils get_edges_domdom -- df que llega ..." )
    # df.show()
    # df.printSchema()

    # this is only for fitler all suspicius domains
    '''
    df_edges_DD_exists = df.select( df.a, df.c,
                                    F.when( df['edge_ratio'] > 0.5, 1 ).otherwise( 0 ).alias(
                                        "edge_ratio" ) )  # .show()
                                        '''
    '''getting the complete graph with suspicious and normal nodes :'''
    # df_edges_DD_exists = df.select( df.a, df.c,
    #                               F.lit(1).alias(
    #                                   "edge_ratio" ) )  # .show()
    df_edges_DD_exists = df.select( df.a, df.c, df.edge_ratio )

    # df_edges_DD_exists.printSchema()

    # print( "df_utils get_edges_domdom -- antes get_edges ..." )

    df_edges = get_edges( df_edges_DD_exists, "a", "c", "edge_ratio" )

    print( "df_utils get_edges_domdom -- df_edges calculado ..." )
    df_edges.show()

    return df_edges


def get_edges_domdom_malicious_ones(df, neighbor=None):
    """
    Creating a df_edges to use GraphFrames only with the suspicious domains
    :param df: dataframe from our data. Idem format like in get_vertices function. df_degree_ratio
    :param neighbor : minimum number of neighbors of the node.
    :return: df_edges
    """
    if f"{neighbor}" is None:
        neighbor == f"{neighbor}"
    else:
        neighbor = 5

    print( "df_utils get_edges_domdom -- df que llega ..." )
    df.show( 5, False )
    df.printSchema()

    # this is only for fitler all suspicius domains
    print( f"NEIGHBOR: {neighbor}" )
    df_filtered_neighbor = df.filter( F.col( 'outDegree' ) > f"{neighbor}" )
    print( "df_utils get_edges_domdom_malicious_ones --df_filtered_neighbor.show()" )
    df_filtered_neighbor.show( 20, False )
    df_filtered_ratio = df_filtered_neighbor.filter( F.col( 'edge_ratio' ) > 0.5 )
    print( "df_utils df_otro -- df que llega ..." )
    df_filtered_ratio.show( 20, False )

    # df_edges_DD_exists = df_filtered_neighbours.select( df.a, df.c,
    #                                                    F.when( df_filtered_neighbors['edge_ratio'] > 0.5,
    #                                                            1 ).otherwise( 0 ).alias(
    #                                                        "edge_ratio" ) )  # .show()

    ##df_filtered_neighbor = df.select( df.a, df.c, F.when( df['edge_ratio'] > 0.5, df['edge_ratio'] ).alias(
    ##                                                        "edge_ratio" ), F.when(df['count_ips_in_common'] > 5, df['count_ips_in_common'] ).alias(
    ##                                                        "count_ips_in_common" ))

    df_edges_DD_exists = df_filtered_ratio.select( df.a, df.c, df.edge_ratio )

    # print( "df_utils get_edges_domdom -- antes get_edges ..." )

    df_edges = get_edges( df_edges_DD_exists, "a", "c", "edge_ratio" )

    print( "df_utils get_edges_domdom_malicious_ones -- df_edges calculado MALICIOUS..." )
    df_edges.show()

    return df_edges
