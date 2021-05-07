from CompareCollection import CompareCollection
from DBConnector import DBConnector
from guppy import hpy
from operator import itemgetter
import time
import json


class Benchmark:
    def __init__(self):
        self.benchmark_result = {
            'Build collection source time performance': [],
            'Build collection source time monotonic': [],
            'Build collection target time performance': [],
            'Build collection target time monotonic': [],
            'Compare collection time performance': [],
            'Compare collection time monotonic': [],
            'Memory usage source': [],
            'Memory usage target': [],
            'Whole benchmark time performance': [],
            'Whole benchmark time monotonic': [],
            'JSON correctness': []
        }

        with open('config.json', 'r') as file:
            config = json.load(file)
        self.db = DBConnector(dbtype=config['DBMS'],
                              host=config['ip'],
                              port=config['port'],
                              dbname=config['db'],
                              user=config['user'],
                              password=config['password'])

    @staticmethod
    def testBuildCollection(collection: CompareCollection, testData):
        h = hpy()

        start_perf = time.perf_counter_ns()
        start_monotonic = time.monotonic_ns()
        collection.build(testData)
        end_perf = time.perf_counter_ns()
        end_monotonic = time.monotonic_ns()

        return str(end_perf - start_perf), str(end_monotonic - start_monotonic), h.iso(collection).indisize

    @staticmethod
    def testCompareCollection(collectionSource: CompareCollection, collectionTarget: CompareCollection):
        start_perf = time.perf_counter_ns()
        start_monotonic = time.monotonic_ns()
        differance = collectionSource.compare(collectionTarget)
        end_perf = time.perf_counter_ns()
        end_monotonic = time.monotonic_ns()

        return str(end_perf - start_perf), str(end_monotonic - start_monotonic), differance

    def testCDC(self, collection, experiment_id=2):
        gen_data = self.db.getExperimentsData(experiment_id)

        for data in gen_data:
            collectionSource = collection()
            collectionTarget = collection()

            start_perf = time.perf_counter_ns()
            start_monotonic = time.monotonic_ns()

            results = self.testBuildCollection(collectionSource, data[0])
            self.benchmark_result['Build collection source time performance'].append(results[0])
            self.benchmark_result['Build collection source time monotonic'].append(results[1])
            self.benchmark_result['Memory usage source'].append(results[2])

            results = self.testBuildCollection(collectionTarget, data[1])
            self.benchmark_result['Build collection target time performance'].append(results[0])
            self.benchmark_result['Build collection target time monotonic'].append(results[1])
            self.benchmark_result['Memory usage target'].append(results[2])

            results = self.testCompareCollection(collectionSource, collectionTarget)
            self.benchmark_result['Compare collection time performance'].append(results[0])
            self.benchmark_result['Compare collection time monotonic'].append(results[1])
            changesExample = sorted(data[2], key=itemgetter('id'))
            changesTest = sorted(results[2], key=itemgetter('id'))
            self.benchmark_result['JSON correctness'].append(changesTest == changesExample)

            end_perf = time.perf_counter_ns()
            end_monotonic = time.monotonic_ns()

            self.benchmark_result['Whole benchmark time performance'].append(str(end_perf - start_perf))
            self.benchmark_result['Whole benchmark time monotonic'].append(str(end_monotonic - start_monotonic))

        return self.benchmark_result
