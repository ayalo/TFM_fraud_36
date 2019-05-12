from pyspark import *
from pyspark.sql import *
from pyspark.sql import functions as F
from graphframes import *
from graphframes.examples import Graphs
from igraph import *
from matplotlib import pyplot as plt
from matplotlib import *
from src.main.python.bipartite_graph import *



#https://towardsdatascience.com/graphframes-in-jupyter-a-practical-guide-9b3b346cebc5
#https://stackoverflow.com/questions/39261370/unable-to-run-a-basic-graphframes-example
spark = SparkSession.builder.appName('drawing graphs').getOrCreate()


def build_vertices(spark):
    vertices = spark.createDataFrame([(1, 'example.org'),
                                      (2,'site.com'),
                                       (3,'example.org'),
                                       (4,'example.org'),
                                       (5,'website.com'),
                                       (6,'website.com'),
                                       (7,'website.com'),
                                       (8,'example.org'),
                                       (9,'example.org'),
                                       (10,'example.org'),
                                       (11,'10.20.30.40'),
                                       (12,'30.50.70.90'),
                                       (13,'10.20.30.41'),
                                       (14,'10.20.30.42'),
                                       (15,'90.80.70.10'),
                                       (16,'30.50.70.90'),
                                       (17,'30.50.70.90'),
                                       (18,'10.20.30.42'),
                                       (19,'10.20.30.40'),
                                       (20,'10.20.30.40')],
                                       ['id','nodos'])
    return vertices

def build_edges(spark):
    edges = spark.createDataFrame([('example.org','10.20.30.40','3'),
                                    ('example.org','10.20.30.42','2'),
                                    ('website.com','90.80.70.10','1'),
                                    ('website.com','30.50.70.90','2'),
                                    ('example.org','10.20.30.41','1'),
                                    ('site.com','30.50.70.90','1')],
                                    ['src', 'dst', 'count'])
    return edges


#with F.SparkContext( "local", "PySparCreateDataframe" ) as spark:
sqlContext = SQLContext( spark )

edges = build_edges(spark)
print ("despues de edges")
vertices = build_vertices(spark)
print ("despues de vertices")

g = GraphFrame(vertices, edges)

## Take a look at the DataFrames
g.vertices.show()
g.edges.show()
## Check the number of edges of each vertex
g.degrees.show()
igraph=Graph.TupleList(g.edges.collect(), directed=True)
plot(igraph)

#igb=Graph.Bipartite(g.vertices.collect(), g.edges.collect(), directed=True)
#plot(igb)
#idb= loadBipartiteGraph(g.edges, g.vertices,)

#gb=Graph.Bipartite(['example.org','site.com','example.org','example.org','website.com','website.com','website.com','example.org','example.org','example.org','10.20.30.40','30.50.70.90','10.20.30.41','10.20.30.42','90.80.70.10','30.50.70.90','30.50.70.90','10.20.30.42','10.20.30.40','10.20.30.40'],g.edges.collect(),directed=True)
#gb.is_bipartite()
#gb.vs["type"]
#print (" Plot GB BIPARTITE:")
#plot(gb)


print(" Graphs example to plot GraphFrame  :")
g2= Graphs(spark).friends()
print(" Graphs example friends vertices:")
g2.vertices.show()
print(" Graphs example friends edges:")
g2.edges.show()
print(" Graphs example display:")
# display(g) #en scala, para python ?
#gx=g2.toGraphX()
ig=Graph.TupleList(g2.edges.collect(), directed=True)
#plot(ig)

##...........................................................................
## para arreglar BUG https://github.com/matplotlib/matplotlib/issues/13414
## $ mkdir -p ~/.matplotlib
#$ echo "backend: TkAgg" > ~/.matplotlib/matplotlibrc
##mas el siguiente codigo:
import matplotlib
matplotlib.use("TKAgg")
print(matplotlib.get_backend())

from matplotlib import pyplot as plt

#plt.plot()
#plt.show()
##...........................................................................

x=[1,2,3,4,5,6,7,8,9]
y=[0.1,2,0.3,4,5,0.6,7,80,0.9]

#plt.boxplot(x,y)
#plt.show()


