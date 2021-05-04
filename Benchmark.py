from CompareCollection import CompareCollection
import timeit
from guppy import hpy


def testBuildCollection(collection: CompareCollection):
    h = hpy()
    # TODO: change dataset
    data = ['334563', '4345634', '5645', '123', '13444']
    print("Time", timeit.timeit(lambda: collection.build(data), number=1))
    print(h.iso(collection))


def testBuildCollection(collection: CompareCollection):
    testBuildCollection(collection)
