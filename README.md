
# TFM_fraud_36

## Development of novel algorithms for fraud detection in online advertising
 
 This proyect is a proof of concept for detecting malicious webs in large datasets 
 based on the paper : **"Using Co-Visitation Networks For Detecting Large Scale Online Display Advertising Exchange Fraud"**
 
 The goal of the project is to implement new algorithms proposed in the literature, 
 including entropy-based methods,co-visitation patterns, etc.  and test the algorithms 
 in large datasets.
 Thanks to a set of logs, we will try to draw patterns of behavior between domains 
 considered malicious, based on the co-visitation degree  and  relationships  between 
 domains  with  similar  content  andcomparing them with domains considered lawful.
  
## Getting Started

Download the full code. 

The following instructions will let you execute the 3 main python classes described below locally for development and testing purposes. 

### Build with or Prerequisites

This proyect was developed in Python using  ```Pycharm Build#PC-183.5912.18```

Also this packages were installed: 

```
conda          4.6.12 
Python         3.6.8 
graphframes    0.6                    
pyspark        2.4.0                   
python-igraph  0.7.1.post7   
```

Be carefull with the environment variables :

|    variable           |    path                                                                                           | 
| ----------------------|:-------------------------------------------------------------------------------------------------:| 
| SPARK_HOME            | .../spark-2.4.1-bin-hadoop2.7/                                                                    | 
| PYTHONPATH            |  .../spark-2.4.1-bin-hadoop2.7/python; .../spark-2.4.1-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip|  
| PYSPARK_PYTHON        | .../anaconda3/envs/fraud_36/bin/python                                                            | 
| PYSPARK_DRIVER_PYTHON | .../anaconda3/envs/fraud_36/bin/python                                                            | 
| PYSPARK_SUBMIT_ARGS   | --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11  pyspark-shell                           | 


Also for generating the folder structure, was used pybuilder and tree. 

### Running the notebooks 
In order to run the Jupiter notebook locally : 
```
PYSPARK_DRIVER_PYTHON=jupyter 
PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip="localhost" --port=8888' 
.../spark-2.4.1-bin-hadoop2.7/bin/pyspark  --jars .../spark-2.4.1-bin-hadoop2.7/jars/graphframes-assembly-0.7.1-SNAPSHOT-spark2.4.jar
```


### Code Organization 

The proyect structre looks like the following image : 

<img src="https://github.com/ayalo/TFM_fraud_36/blob/master/docs/images/tree_fraud36.png" width="350" height="350">

Three main clases into the **fraud36/source/** folder, allow us to execute the code to obtain, different graphs and plots. 

Into the folder **fraud36/notebooks**, there are 3 Jupyter Notebooks 

with some tests to different datasets and classes for the **'..._main'.py** previously cited.

Into the folder **fraud36/utils**, there are code distributed in classes to use in different data structures; for example : **df_utils.py** contains the functions that 
apply to an spark dataframe; **gf_utils.py** contains functions that apply to a graphframes graph; **draw_utils.py** function to plot and draw ... 

<img src="https://github.com/ayalo/TFM_fraud_36/blob/master/docs/images/utils_fraud36.png" width="200" height="100">

Also there is a zip : **'fraud36/source/utils.zip'** in order to make the use of this functions easy using a Jupyter Notebook. 


<img src="https://github.com/ayalo/TFM_fraud_36/blob/master/docs/images/algorithm_code_schema.png" width="200" height="200">


<img src="https://github.com/ayalo/TFM_fraud_36/blob/master/docs/images/functions_code.png" width="200" height="200">


## Functions described :

### df_utils.py :

def **is_string(s)**:
```
    Function that returns true if s is a string or false if not
    The intention of this function is filter all the types into the df that have no normalized type in 'referrer_domain'
    or 'user_ip' columns, in order to clean the original dataset.
    :param s: string passed as an argument
    :return: boolean True/False
```
def **clean(df, referrer_domain, user_ip)**:  
```
    Function to clean the original data from df, eliminate or filter null, none, and other types that are not string in
    'referrer_domain' or 'user_ip' columns, in order to clean the original dataset.
    :param df:
    :param referrer_domain: first column to clean
    :param user_ip: second column to clean
    :return: df_cleaned_format
```

def **get_vertices(df_edges, a, b)**:
```
    Function to Rename the columns of  df_vertices in order to use GraphFrames [id].
    This function renames the columns a and b with the alias id, and returns the dataframe df_vertices,
    with a single column and with dropedDuplicates.
    :param df_edges: dataframe of edges with format for a GraphFrames use (src,dst,edge_weight)
    :param a: df column
    :param b: df column
    :return:
```

def **get_edges(df, a, b, c)**:
```
    Function to Rename the columns of df_edges in the correct format to GraphFrames [src, dst, edge_weight]
    :param df: df_edges with the incorrect name columns
    :param a: old name for column src
    :param b: old name for column dst
    :param c: old name for column edge_weight
    :return: df_edges
```

def **get_edges_domip(df)**:
```
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
```

def **get_edges_domdom(df)**:
```
    Creating a df_edges to use GraphFrames in order to create a domain-domain graph.
    :param df: dataframe from our data. Idem format like in get_vertices function.
    :return: df_edges
```

def **get_edges_domdom_malicious_ones(df)**:
```
    Creating a df_edges to use GraphFrames only with the suspicious domains
    :param df: dataframe from our data. Idem format like in get_vertices function.
    :return: df_edges
```



### draw_utils.py :

def **draw_nx(df_edges,path=None)**:  # used in domain-ip  and dom-dom graphs, doesn't work with huge amount of data
```
    Function to plot a bipartite graph with networkx
    :param df_edges: df_edges from a GraphFrame
    :param path=None
    :return: ploted graph

```
def **draw_igraph_domain_domain(g_domdom)**:
```
    Version: 1.0
    Function to plot a dispersion of nodes (TupleList) in a graph with igraph
    :param g: GraphFrame
    :return: ig,visual_style : igraph and visual_style to draw the graph
```

def **draw_igraph_domain_ip(g)**:
    #TODO es repetida de la anterior, intentar mejorarla o borrarla
```
       Version: 1.0
       Function to plot a dispersion of nodes (TupleList) in a graph with igraph
       :param g: GraphFrame
       :return: ig,visual_style : igraph and visual_style to draw the graph
```

def **draw_log_hist(degree, bins=10,path=None)**:
```
    Function to draw a histogram in logaritmic scale.

    :param degree : node degree
    :param bins   : division of the histogram, dos methods
    :param path   : path where to save the histogram image
    :return : ploted bar histogram

    draw_log_hist(degree,10):
    returns 10 bars with division made by np.histogram

    draw_log_hist(degree,[1,10,100,200]):
    returns plot between the numbers passed as a parameter,
    it means that sums the number of elements between 1-10, 10-100,100-200 ....

    LOGARITHMIC SCALE

```

def **draw_minor_than_list(degree, list_tope,path=None)**:
```

    Function to represent the elements that are minor than a maximum (tope),
    how many are minor than 400, minor than 300, minor than 200 ....
    :param degree   : degree a pintar
    :param list_tope: array of integers with the maximums
    :param path     : path where to save the histogram image
    :return ploted bar histogram
```

def **draw_overlap_matrix(df_degree_ratio, list_top_suspicious,figsize=(10,10),path=None)**:
```
    Function to draw an overlap matrix of suspicious domains
    :param df_degree_ratio : dataframe with all the data needed to represent the overlap matrix [a,c,count_ips_in_common,id,outDegree,edge_ratio]
                             where a is src, c is dst, id is src, and outDegree is the outDegree of src. The edge_ratio is calculated with the
                            algorithm proposed.
    :param top_suspicious : number of top suspicious domains to plot
    :param figsize
    :param path : path where to save the histogram image
    :return ploted overlap matrix
```




### gf_utils.py :

def **gf_filter_dom_ip_edges(g, min_edge)**:  # filtar count>15 visitas  en el grafo. # Usada en DI y DD
```
    Function to select only the nodes in the graph with more than a given weight passed as parameter, in order
    to filter non relevant data to construct an smaller graph .
    :param g: original gf_dom_ip GraphFrame
    :param min_edge: value to dismiss all the nodes on g below the limit_edge value . limit_edge value indicate the
            number of visits domain-ip (if for a src - dst tuple : edge_weight <  limit_edge this row is discarded)
    :return gf_filtered: GraphFrame generated with
```

def **get_graph_domip(df, min_edge)**:
```
    Get GraphFrame to draw a bipartite graph
    :param df dataframe from our data. Idem format like in get_vertices function.
    :param min_edge: value to dismiss all the nodes on g below the min_edge value
                        (if for a src - dst tuple : edge_weight <  min_edge this row is discarded)
    :return: gf_filtered (GraphFrame graph) filtered by min_edge

    :definition df_vertices: vertices for the graphframe : domains and ips
    :definition df_edges: links between them
```

def **get_motifs(g_domip)**:
```
    Query graph to get motifs df of ip_cleaned IPs visited by 2 different domain_cleaned domains

        Motifs finding is also known as graph pattern matching.
        The pattern matching consists on checking a value against some pattern.
        The pattern is an expression used to define some connected vertices.

    :param g_domip:  graphframe
    :return: df_motifs
```

def **get_motifs_count(df_motifs)**:
```
    Get the count of ip_cleaned IPs visisted by 2 different domain_cleaned domains

    :param df_motifs:
    :return: df_motifs_count
```

def **get_df_degree_ratio(g_domip)**:
```
    Function to get the df_degree_ratio, with the format described below.
    :param g_domip:
    :return df_degree_ratio : dataframe with all the data needed to represent the overlap matrix
    [a,c,count_ips_in_common,id,outDegree,edge_ratio]
    where a is src, c is dst, id is src, and outDegree is the outDegree of src.
    The edge_ratio is calculated with the algorithm proposed.
```

def **get_graph_domdom(g_domip)**:
```
    Get GraphFrame to draw graph
    :param g_domip: graph from domain_ip_graph .
    :return: gf (GraphFrame graph) domain-domain
    :definition df_vertices: vertices for the graphframe : domains
    :definition df_edges: links between them
```

def **gf_top_most_visited(gf, top=None)**:
```
    Using the graphframe function 'degrees', calculate the most visited vertices.
    :param gf:  grapframe
    :return: sorted_degrees
```

def **gf_filter_edge(gf, src)**:
```
    Function to get the subgraph of Graphframes graph related of a src (domain) gived as a function parameter
    :param gf:
    :param src:
    :return: subgraph for the src
```


### read_write_utils.py :

def **gf_write_parquet(gf, path)**:
```
    Function to save (write to disk) a graphframe gf using write.parquet
    :param gf: graph graphframe
    :param path: path where to save the graph
    :return:  - saved graph into the path given
```

def **gf_read_parquet(spark_session, path)**:
```
    Function to read a graph graphframes saved into disk, using read.parquet
    :param spark_session:
    :param path: where to read the graph saved
    :return: graphframe graph
```

def **df_write_parquet(df, path)**:
```
    Function to save (write to disk) a dataframe df using write.parquet
    :param df: dataframe
    :param path: path where to save the dataframe
    :return:  - saved dataframe into the path given
```

def **df_read_parquet(spark_session, path)**:
```
    Function to read a df dataframe saved into disk, using read.parquet
    :param spark_session:
    :param path: where to read the df saved
    :return: dataframe
```

def **gf_write_csv(gf, path)**:
```
    Function to save (write to disk) a graphframe gf using write.csv
    :param gf: graph graphframe
    :param path: path where to save the graph
    :return:  - saved graph into the path given
```

def **gf_read_csv(spark_session, path)**:
```
    Function to read a graph graphframes saved into disk, using read.csv
    :param spark_session:
    :param path: where to read the graph saved
    :return: graphframe graph
```

def **df_write_csv(df, path)**:
```
    Function to save (write to disk) a dataframe df using write.csv
    :param df: dataframe
    :param path: path where to save the dataframe
    :return:  - saved dataframe into the path given
```

def **df_read_csv(spark_session, path)**:
```
    Function to read a df dataframe saved into disk, using read.csv
    :param spark_session:
    :param path: where to read the df saved
    :return: dataframe
```

### row_cleaners_utils.py :

def **domain_cleaner(domain)**:
```
    Function to clean a domain passed as a parameter string.

    We left only lower case, clean certain characters and only printable ones
    We get only the main domain, or the first part til the first / begging from the left
    We substract '.com' from the begging and 'https://' 'http://' 'www.'
    :param domain : String
    :return: cleaned_domain
```
    
def **delete_ini(text, subString)**:
```
    Function to recursively delete the first part of an string, for example
    if the text or domain starts like : com.com.com.google.es, we only left google.es

    :param text:
    :param subString:
    :return:
```

def **ip_cleaner(ip)**:
```
    To verify that the IP has this format  : X.X.X.X
    :arg ip : String
    :return: cleaned_ip / "Format not valid"
```

def **valid_ip(ip)**:
```
    To verify that the IP has this format  : X.X.X.X
    :param ip:
    :return: Boolean
```

### spark_utils.py :
```
def **spark_session()**:
    return SparkSession.builder.getOrCreate()
```

