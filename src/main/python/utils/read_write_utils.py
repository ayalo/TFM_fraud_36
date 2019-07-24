from graphframes import *

EDGES_WRITED = "edges_writed"
VERTICES_WRITED = "vertices_writed"


def gf_write_parquet(gf, path):
    gf.edges.write.parquet( f"{path}/{EDGES_WRITED}" )
    gf.vertices.write.parquet( f"{path}/{VERTICES_WRITED}" )
    print( f" Salvado parquet grafo en el path :{path}" )


def gf_read_parquet(spark_session, path):
    ed = spark_session.read.parquet( f"{path}/{EDGES_WRITED}" )
    ver = spark_session.read.parquet( f"{path}/{VERTICES_WRITED}" )
    print( f" Leemos parquet el grafo  en el path :{path}" )

    return GraphFrame( ver, ed )


def df_write_parquet(df, path):
    df.write.parquet( f"{path}" )
    print( f" Salvado parquet df en el path :{path}" )


def df_read_parquet(spark_session, path):
    print( f" Leemos parquet  df  en el path :{path}" )
    return spark_session.read.parquet( f"{path}" )



def gf_write_csv(gf, path):
    gf.edges.write.csv( f"{path}/{EDGES_WRITED}" )
    gf.vertices.write.csv( f"{path}/{VERTICES_WRITED}"  )
    print( f" Salvado CSV grafo en el path :{path}" )


def gf_read_csv(spark_session, path):
    ed = spark_session.read.csv( f"{path}/{EDGES_WRITED}", header=True )
    ver = spark_session.read.csv( f"{path}/{VERTICES_WRITED}", header=True )
    print( f" Leemos CSV el grafo  en el path :{path}" )
    return GraphFrame( ver, ed )


def df_write_csv(df, path):
    df.write.csv( f"{path}"  )
    print( f" Salvado CSV df en el path :{path}" )


def df_read_csv(spark_session, path):
    print( f" Leemos CSV  df  en el path :{path}" )
    return spark_session.read.csv( f"{path}" , header=True )
