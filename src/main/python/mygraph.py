from pyspark import *
from pyspark.sql import *

from graphframes import *

from matplotlib import pyplot as plt
from matplotlib import *

#https://towardsdatascience.com/graphframes-in-jupyter-a-practical-guide-9b3b346cebc5
#https://stackoverflow.com/questions/39261370/unable-to-run-a-basic-graphframes-example
spark = SparkSession.builder.appName('drawing graphs').getOrCreate()


def build_vertices(spark):

    vertices = spark.createDataFrame([('1', 'Carter', 'Derrick', 50),
                                  ('2', 'May', 'Derrick', 26),
                                 ('3', 'Mills', 'Jeff', 80),
                                  ('4', 'Hood', 'Robert', 65),
                                  ('5', 'Banks', 'Mike', 93),
                                 ('98', 'Berg', 'Tim', 28),
                                 ('99', 'Page', 'Allan', 16)],
                                 ['id', 'name', 'firstname', 'age'])

    return vertices


def build_edges(spark):
    edges = spark.createDataFrame([('1', '2', 'friend'),
                               ('2', '1', 'friend'),
                              ('3', '1', 'friend'),
                              ('1', '3', 'friend'),
                               ('2', '3', 'follows'),
                               ('3', '4', 'friend'),
                               ('4', '3', 'friend'),
                               ('5', '3', 'friend'),
                               ('3', '5', 'friend'),
                               ('4', '5', 'follows'),
                              ('98', '99', 'friend'),
                              ('99', '98', 'friend')],
                              ['src', 'dst', 'type'])

    return edges



edges = build_edges(spark)

vertices = build_vertices(spark)

g = GraphFrame(vertices, edges)


## Take a look at the DataFrames
g.vertices.show()
g.edges.show()
## Check the number of edges of each vertex
g.degrees.show()



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

plt.boxplot(x,y)
plt.show()

plt.boxplot(x,y)
plt.show()

