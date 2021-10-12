from typing import Any, NoReturn
from cdc_benchmark.data_base_connector import DBConnector
from collections.abc import Mapping, Iterable
from typing import Tuple
from datetime import datetime
from sys import getsizeof, executable
from subprocess import check_call
import importlib
import time


class Benchmark:
    def __init__(self, config: dict) -> NoReturn:
        self.table_source = config['table_source']
        self.table_destination = config['table_destination']
        self.changeset_param = config['Changeset_param']

        # TODO: handle exception
        self.benchDB = DBConnector(config['benchDB'])

        if config['benchDB'] == config['destination']:
            self.destinationDB = self.benchDB
        else:
            self.destinationDB = DBConnector(config['destination'])

        if config['destination'] == config['source']:
            self.sourceDB = self.destinationDB
        elif config['benchDB'] == config['source']:
            self.sourceDB = self.benchDB
        else:
            self.sourceDB = DBConnector(config['source'])

        SDS_param = config['SDS_param']
        for package in SDS_param['requirements']:
            check_call([executable, "-m", "pip", "install", package])
        spec = importlib.util.spec_from_file_location(SDS_param['module'], SDS_param['path'])
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        testing_class = getattr(module, SDS_param['class'])

        self.source_struct = testing_class(hsh=SDS_param['hash'])
        self.destination_struct = testing_class(hsh=SDS_param['hash'])

        self.name = SDS_param["class"] + "_" + SDS_param["hash"] + "_" + self.table_source['name'] + "_" + \
                    self.table_destination['name']

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
    def build_struct(db: DBConnector, table: dict, struct: Any) -> Tuple[int, str]:
        print(f'Start getting data from table {table["name"]}')
        data = db.getTableData(table_name=table['name'], pk=table['PK'])
        start_perf = time.monotonic_ns()
        count = 0
        for i in data:
            count += len(i['keys'])
            print(f"Get {count} rows! Start adding to struct!")
            struct.add_iter(i['keys'], i['values'])
        end_perf = time.monotonic_ns()
        struct_size = Benchmark.deep_getsizeof(struct)
        result_time_min = Benchmark.ns_min(end_perf - start_perf)

        return struct_size, result_time_min

    def start_benchmarking(self) -> dict:
        result = {}
        efficiency = {}
        start_perf = time.monotonic_ns()
        print("Start test building source struct!")
        efficiency['source size'], efficiency['source build time'] = self.build_struct(
            self.sourceDB,
            self.table_source,
            self.source_struct)
        print("Start test building destination struct!")
        efficiency['destination size'], efficiency['destination build time'] = self.build_struct(
            self.destinationDB,
            self.table_destination,
            self.destination_struct)

        print("Checking equivalents!")
        start_perf_answer = time.monotonic_ns()
        answer = self.source_struct == self.destination_struct
        end_perf_answer = time.monotonic_ns()
        answer_time = Benchmark.ns_min(end_perf_answer - start_perf_answer)
        efficiency['compare time'] = answer_time

        print("Start getting changeset!")
        start_perf_change_table = time.monotonic_ns()
        change_table = self.source_struct.get_changeset(self.destination_struct)
        end_perf_change_table = time.monotonic_ns()
        change_table_time = Benchmark.ns_min(end_perf_change_table - start_perf_change_table)
        efficiency['getting change table time'] = change_table_time
        end_perf = time.monotonic_ns()
        efficiency['benchmark time'] = Benchmark.ns_min(end_perf - start_perf)

        if self.changeset_param['content']['answer']:
            result['compare_answer'] = answer

        if self.changeset_param['content']['changetable']:
            result['change table'] = change_table

        if self.changeset_param['content']['efficiency']:
            result['efficiency'] = efficiency

        return result
