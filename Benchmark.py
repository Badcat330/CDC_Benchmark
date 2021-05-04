from CompareCollection import CompareCollection
from DBConnector import DBConnector
from guppy import hpy
import time
import json


class Benchmark:
    def __init__(self):
        self.benchmark_result = {
            'Compare collection time performance': '',
            'Compare collection time monotonic': '',
        }

        with open('config.json', 'r') as file:
            config = json.load(file)
        self.db = DBConnector(dbtype=config['DBMS'],
                              host=config['ip'],
                              port=config['port'],
                              dbname=config['db'],
                              user=config['user'],
                              password=config['password'])

    def testBuildCollection(self, collection: CompareCollection):
        # TODO: Check experiment flow

        gen_data = self.db.getExperimentsData(1)
        h = hpy()

        start_perf = time.perf_counter_ns()
        start_monotonic = time.monotonic_ns()
        collection.build(next(gen_data))
        end_perf = time.perf_counter_ns()
        end_monotonic = time.monotonic_ns()

        self.benchmark_result['Build collection time performance'] = str(end_perf - start_perf)
        self.benchmark_result['Build collection time monotonic'] = str(end_monotonic - start_monotonic)
        self.benchmark_result['Memory usage'] = h.iso(collection)

    def testCompareCollection(self, collection: CompareCollection):
        # TODO: Implement
        pass

    def testCDC(self, collection: CompareCollection):
        start_perf = time.perf_counter_ns()
        start_monotonic = time.monotonic_ns()
        self.testBuildCollection(collection)
        self.testCompareCollection(collection)
        end_perf = time.perf_counter_ns()
        end_monotonic = time.monotonic_ns()
        self.benchmark_result['Whole benchmark time performance'] = str(end_perf - start_perf)
        self.benchmark_result['Whole benchmark time monotonic'] = str(end_monotonic - start_monotonic)

        return self.benchmark_result
