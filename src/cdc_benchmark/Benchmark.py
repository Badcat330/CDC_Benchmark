from typing import Any, NoReturn
from cdc_benchmark.data_base_connector import DBConnector
from collections.abc import Mapping, Iterable
from datetime import datetime
from sys import getsizeof
import importlib
import time


class Benchmark:
    def __init__(self, config: dict) -> NoReturn:
        self.table_source = config['table_source']
        self.table_destination = config['table_destination']
        self.changeset_param = config['Changeset_param']

        # TODO: handle exception
        self.benchDB = DBConnector(config['benchDB'])
        self.destinationDB = DBConnector(config['destination'])
        self.sourceDB = DBConnector(config['source'])

        SDS_param = config['SDS_param']
        spec = importlib.util.spec_from_file_location(SDS_param['module'], SDS_param['path'])
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        testing_class = getattr(module, SDS_param['class'])

        self.source_struct = testing_class(hash=SDS_param['hash'])
        self.destination_struct = testing_class(hash=SDS_param['hash'])

    @staticmethod
    def deep_getsizeof(o: object, ids: set = None) -> int:
        """Find the memory footprint of a Python object
        This is a recursive function that rills down a Python object graph
        like a dictionary holding nested ditionaries with lists of lists
        and tuples and sets.
        The sys.getsizeof function does a shallow size of only. It counts each
        object inside a container as pointer only regardless of how big it
        really is.
        Made by https://github.com/the-gigi
        :param o: the object
        :param ids: temporary conatiner for id
        :return: size in bytes
        """
        if ids is None:
            ids = set()

        d = Benchmark.deep_getsizeof
        if id(o) in ids:
            return 0

        r = getsizeof(o)
        ids.add(id(o))

        if hasattr(o, '__dict__'):
            v = o.__dict__
            return r + sum(d(v[k], ids) for k in v)

        if isinstance(o, str):
            return r

        if isinstance(o, Mapping):
            return r + sum(d(k, ids) + d(o[k], ids) for k in o)

        if isinstance(o, Iterable):
            return r + sum(d(x, ids) for x in o)

        return r

    @staticmethod
    def ns_min(time_ns: int) -> str:
        result_time_min = datetime.fromtimestamp(time_ns // 1000000000)
        result_time_min = result_time_min.strftime('%M:%S')
        result_time_min += '.' + str(int(time_ns % 1000000000)).zfill(9)
        return result_time_min

    @staticmethod
    def build_struct(db: DBConnector, table: dict, struct: Any) -> tuple[int, str]:
        data = db.getTableData(table_name=table['name'], pk=table['PK'])
        start_perf = time.perf_counter_ns()
        struct.add_iter(data['keys'], data['values'])
        end_perf = time.perf_counter_ns()
        struct_size = Benchmark.deep_getsizeof(struct)
        result_time_min = Benchmark.ns_min(end_perf - start_perf)

        return struct_size, result_time_min

    def start_benchmarking(self) -> dict:
        result = {}
        efficiency = {}
        start_perf = time.perf_counter_ns()
        print("Strart building source struct!")
        efficiency['source size'], efficiency['source build time'] = self.build_struct(
            self.sourceDB,
            self.table_source,
            self.source_struct)
        print("Strart building destination struct!")
        efficiency['destination size'], efficiency['destination build time'] = self.build_struct(
            self.sourceDB,
            self.table_destination,
            self.destination_struct)

        if self.changeset_param['content']['answer']:
            print("Checking equvalens!")
            start_perf_answer = time.perf_counter_ns()
            answer = self.source_struct == self.destination_struct
            end_perf_answer = time.perf_counter_ns()
            answer_time = Benchmark.ns_min(end_perf_answer - start_perf_answer)
            result['compare_answer'] = answer
            efficiency['compare time'] = answer_time

        if self.changeset_param['content']['changetable']:
            print("Start getting changeset!")
            start_perf_change_table = time.perf_counter_ns()
            change_table = self.source_struct.get_changeset(self.destination_struct)
            end_perf_change_table = time.perf_counter_ns()
            change_table_time = Benchmark.ns_min(end_perf_change_table - start_perf_change_table)
            result['change table'] = change_table
            efficiency['getting change table time'] = change_table_time
        end_perf = time.perf_counter_ns()

        efficiency['benchmark time'] = Benchmark.ns_min(end_perf - start_perf)

        if self.changeset_param['content']['efficiency']:
            result['efficiency'] = efficiency

        return result
