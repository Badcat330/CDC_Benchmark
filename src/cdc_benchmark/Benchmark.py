from typing import Any
from cdc_benchmark.data_base_connector import DBConnector
from collections import Mapping, Container
from datetime import datetime
from sys import getsizeof
import importlib
import time




class Benchmark:
    def __init__(self, config: dict) -> None:
        self.table_source = config['table_source']
        self.table_destination = config['table_destination']
        self.changeset_param = config['Changeset_param']

        #TODO: handle exception
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
    def deep_getsizeof(o, ids=set()):
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
        d = Benchmark.deep_getsizeof
        if id(o) in ids:
            return 0

        r = getsizeof(o)
        ids.add(id(o))

        if isinstance(o, str):
            return r

        if isinstance(o, Mapping):
            return r + sum(d(k, ids) + d(v, ids) for k, v in o.iteritems())

        if isinstance(o, Container):
            return r + sum(d(x, ids) for x in o)

        return r

    @staticmethod
    def ns_min(time_ns):
        retult_time_min = datetime.fromtimestamp(time_ns // 1000000000)
        retult_time_min = retult_time_min.strftime('%M:%S')
        retult_time_min += '.' + str(int(time_ns % 1000000000)).zfill(9)
        return retult_time_min

    def build_struct(self, db: DBConnector, table: dict, struct: Any) -> tuple[int, str]:
        data = db.getTableData(table_name=table['name'], pk=table['PK'])

        start_perf = time.perf_counter_ns()
        struct.add_iter(data['key'], data['value'])
        end_perf = time.perf_counter_ns()

        struct_size = Benchmark.deep_getsizeof(struct)
        retult_time_min = Benchmark.ns_min(end_perf - start_perf)

        return struct_size, retult_time_min

        

    def start_benchmarking(self):
        result = {}
        efficency = {}
        start_perf = time.perf_counter_ns()
        
        efficency['source size'], efficency['source build time'] = self.build_struct(
            self.sourceDB, 
            self.table_source, 
            self.source_struct)
        efficency['destination size'], efficency['destination build time'] = self.build_struct(
            self.destinationDB, 
            self.table_destination, 
            self.destination_struct)
        
        if self.changeset_param['content']['answer']:
            start_perf_answer = time.perf_counter_ns()
            answer = self.source_struct == self.destination_struct
            end_perf_answer = time.perf_counter_ns()
            answer_time = Benchmark.ns_min(end_perf_answer - start_perf_answer)
            result['compare_answer'] = answer
            efficency['compare time'] = answer_time

        if self.changeset_param['content']['changetable']:
            start_perf_chamgetable = time.perf_counter_ns()
            chamgetable = self.source_struct.get_changeset(self.destination_struct)
            end_perf_chamgetable = time.perf_counter_ns()
            chamgetable_time = Benchmark.ns_min(end_perf_chamgetable - start_perf_chamgetable)
            result['chamgetable'] = chamgetable
            efficency['getting changetable time'] = chamgetable_time
        end_perf = time.perf_counter_ns()

        efficency['benchmark time'] = Benchmark.ns_min(end_perf - start_perf)

        if self.changeset_param['content']['efficency']:
            result['efficency'] = efficency

        return result
            

