

import pytest

#import findspark
#findspark.init()

from  src.main.python import _mygraph



def test_build_vertices(spark):

    vertices = _mygraph.build_vertices( spark )

    vertices.head(10)