from graphframes import *

EDGES_WRITED = "edges_writed"
VERTICES_WRITED = "vertices_writed"


def gf_write_parquet(gf, path):
    """
    Function to save (write to disk) a graphframe gf using write.parquet
    :param gf: graph graphframe
    :param path: path where to save the graph
    :return:  - saved graph into the path given
    """
    gf.edges.write.parquet( f"{path}/{EDGES_WRITED}" )
    gf.vertices.write.parquet( f"{path}/{VERTICES_WRITED}" )
    print( f" Saved parquet graph into path :{path}" )


def gf_read_parquet(spark_session, path):
    """
    Function to read a graph graphframes saved into disk, using read.parquet
    :param spark_session:
    :param path: where to read the graph saved
    :return: graphframe graph
    """
    ed = spark_session.read.parquet( f"{path}/{EDGES_WRITED}" )
    ver = spark_session.read.parquet( f"{path}/{VERTICES_WRITED}" )
    print( f" Readed parquet graph from path :{path}" )

    return GraphFrame( ver, ed )


def df_write_parquet(df, path):
    """
    Function to save (write to disk) a dataframe df using write.parquet
    :param df: dataframe
    :param path: path where to save the dataframe
    :return:  - saved dataframe into the path given
    """
    df.write.parquet( f"{path}" )
    print( f" Saved parquet df into path :{path}" )


def df_read_parquet(spark_session, path):
    """
    Function to read a df dataframe saved into disk, using read.parquet
    :param spark_session:
    :param path: where to read the df saved
    :return: dataframe
    """
    print( f" Readed parquet df from path :{path}" )
    return spark_session.read.parquet( f"{path}" )



def gf_write_csv(gf, path):
    """
    Function to save (write to disk) a graphframe gf using write.csv
    :param gf: graph graphframe
    :param path: path where to save the graph
    :return:  - saved graph into the path given
    """
    gf.edges.write.csv( f"{path}/{EDGES_WRITED}" )
    gf.vertices.write.csv( f"{path}/{VERTICES_WRITED}"  )
    print( f"  Saved CSV graph into path :{path}" )


def gf_read_csv(spark_session, path):
    """
    Function to read a graph graphframes saved into disk, using read.csv
    :param spark_session:
    :param path: where to read the graph saved
    :return: graphframe graph
    """
    ed = spark_session.read.csv( f"{path}/{EDGES_WRITED}", header=True )
    ver = spark_session.read.csv( f"{path}/{VERTICES_WRITED}", header=True )
    print( f" Readed CSV graph from path :{path}" )
    return GraphFrame( ver, ed )


def df_write_csv(df, path):
    """
    Function to save (write to disk) a dataframe df using write.csv
    :param df: dataframe
    :param path: path where to save the dataframe
    :return:  - saved dataframe into the path given
    """
    df.write.csv( f"{path}"  )
    print( f"  Saved CSV df into path :{path}" )


def df_read_csv(spark_session, path):
    """
    Function to read a df dataframe saved into disk, using read.csv
    :param spark_session:
    :param path: where to read the df saved
    :return: dataframe
    """

    print( f" Readed CSV df from path :{path}" )
    return spark_session.read.csv( f"{path}" , header=True )
