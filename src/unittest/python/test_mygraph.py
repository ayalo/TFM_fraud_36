

import pytest

#import findspark
#findspark.init()

from  src.main.python import mygraph



def test_build_vertices(spark):

    vertices = mygraph.build_vertices(spark)

    vertices.head(10)