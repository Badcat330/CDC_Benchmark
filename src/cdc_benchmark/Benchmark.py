from CompareCollection import CompareCollection
from DBConnector import DBConnector
from guppy import hpy
from operator import itemgetter
import time
import json


class Benchmark:
    def __init__(self, dbSave=False):
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
        }

        self.dbSave = dbSave

        self.db = DBConnector()

    @staticmethod
    def testBuildCollection(collection: CompareCollection, testData):
        h = hpy()

        start_perf = time.perf_counter_ns()
        start_monotonic = time.monotonic_ns()
        collection.build(testData)
        end_perf = time.perf_counter_ns()
        end_monotonic = time.monotonic_ns()

        # for x in range(h.heap().size - 1):
        #     print(h.heap()[x])

        return str(end_perf - start_perf), str(end_monotonic - start_monotonic), h.iso(collection).indisize

    @staticmethod
    def testCompareCollection(collectionSource: CompareCollection, collectionTarget: CompareCollection):
        start_perf = time.perf_counter_ns()
        start_monotonic = time.monotonic_ns()
        differance = collectionSource.compare(collectionTarget)
        end_perf = time.perf_counter_ns()
        end_monotonic = time.monotonic_ns()

        return str(end_perf - start_perf), str(end_monotonic - start_monotonic), differance

    def testCDCExperimentSet(self, collection, experiment_id=1):
        gen_data = self.db.getExperimentsData(experiment_id)
        self.benchmark_result["JSON correctness"] = []

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

    def testCDCCompareTables(self, collection, tables):
        gen_data = self.db.getTablesData(tables)

        answerJSON = {
            "Answer": "",
            "Changes table": "",
            "Efficiency": ""
        }

        collectionSource = collection()
        collectionTarget = collection()

        start_perf = time.perf_counter_ns()
        start_monotonic = time.monotonic_ns()

        results = self.testBuildCollection(collectionSource, gen_data[0])
        self.benchmark_result['Build collection source time performance'] = results[0]
        self.benchmark_result['Build collection source time monotonic'] = results[1]
        self.benchmark_result['Memory usage source'] = results[2]

        results = self.testBuildCollection(collectionTarget, gen_data[1])
        self.benchmark_result['Build collection target time performance'] = results[0]
        self.benchmark_result['Build collection target time monotonic'] = results[1]
        self.benchmark_result['Memory usage target'] = results[2]

        results = self.testCompareCollection(collectionSource, collectionTarget)
        self.benchmark_result['Compare collection time performance'] = results[0]
        self.benchmark_result['Compare collection time monotonic'] = results[1]

        if len(results[2]) == 0:
            answerJSON["Answer"] = "Consistent"
        else:
            answerJSON["Answer"] = "Inconsistent"
            answerJSON["Changes table"] = results[2]

        end_perf = time.perf_counter_ns()
        end_monotonic = time.monotonic_ns()

        self.benchmark_result['Whole benchmark time performance'] = str(end_perf - start_perf)
        self.benchmark_result['Whole benchmark time monotonic'] = str(end_monotonic - start_monotonic)

        answerJSON["Efficiency"] = self.benchmark_result

        if self.dbSave:
            self.db.saveResults(answerJSON, tables)

        return answerJSON
