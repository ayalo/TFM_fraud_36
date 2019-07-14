from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import *

from gf_utils.gf_utils import *

from domain_cleaner import domain_cleaner
from ip_cleaner import ip_cleaner


def is_string(s):  # usada en DI y DD
    """
    Function that returns true if s is a string or false if not
    The intention of this function is filter all the types into the df that have no boolean in 'referrer_domain'
    or 'user_ip' columns, in order to clean the original data.
    :param s: string passed as an argument
    :return: boolean True/False
    """
    # return isinstance( s, basestring )
    return type( s ) is str


def clean(df, referrer_domain, user_ip):  # usada en DI (con el df original) y DD (con el df original)
    """
    Function to clean the original data from df, eliminate or filter null, none, and other types that are not string in
    'referrer_domain' or 'user_ip' columns, in order to clean the original data.
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
    print( "clean-- Calculando df_cleaned_ip" )
    df_cleaned_ip = df.withColumn( 'ip_cleaned', udf_ipcleaner( df.user_ip ) )

    # Adding new column to df with the normalized/filtered/cleaned referrer_domain : domain_cleaned
    udf_domaincleaner = udf( domain_cleaner, StringType() )
    df_cleaned = df_cleaned_ip.withColumn( 'domain_cleaned', udf_domaincleaner( df_cleaned_ip.referrer_domain ) )

    ## DROP/Filter Format not valid in ip_cleaned
    df_cleaned_format = df_cleaned.filter( (df_cleaned.ip_cleaned != 'Format not valid') )

    return df_cleaned_format


def get_vertices(df_edges, a, b):
    print( "df_utils get_vertices-- :" )

    df_dom = df_edges.select( col( f"{a}" ).alias( "id" ) )
    df_ip = df_edges.select( col( f"{b}" ).alias( "id" ) )

    print( "df_utils get_vertices-- df_vertices_withduplicates :" )
    df_vertices_withduplicates = df_dom.union( df_ip )
    df_vertices_withduplicates.show()

    print( "df_utils get_vertices-- df_vertices_sin duplicates :" )
    df_vertices = df_dom.union( df_ip ).dropDuplicates()
    df_vertices.show()

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
    Creating a df_edges to use GraphFrames
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
    print( "DomainIpGraph get_edges-- :" )
    df_edges_count = df.groupBy( "domain_cleaned", "ip_cleaned" ).count()
    df_edges = get_edges( df_edges_count, "domain_cleaned", "ip_cleaned", "count" )

    return df_edges


def get_edges_domdom(df):
    """
    Creating a df_edges to use GraphFrames
    :param df: dataframe from our data. Idem format like in get_vertices function.
    :return: df_edges
    """
    print( "df_utils get_edges_domdom -- df que llega ..." )
    df.show()
    # df.printSchema()

    df_edges_DD_exists = df.select( df.a, df.c,
                                    F.when( df['edge_ratio'] > 0.5, 1 ).otherwise( 0 ).alias(
                                        "edge_ratio" ) )  # .show()

    df_edges_DD_exists.printSchema()

    print( "df_utils get_edges_domdom -- antes get_edges ..." )

    df_edges = get_edges( df_edges_DD_exists, "a", "c", "edge_ratio" )
    print( "df_utils get_edges_domdom -- df_edges calculado ..." )
    df_edges.show()

    return df_edges
