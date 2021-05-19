from Benchmark import Benchmark
import sys
import json
import argparse
import os
import importlib
import importlib.util

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", "-p", help="Add profile information in json if it is given else it shows exist "
                                                "one.", nargs='?', const=' ')
    parser.add_argument("--template", "-t", help="Create profile template json.")
    parser.add_argument("--file", "-f", help='Option for saving results in json file.'
                                             ' Required path for saving file')
    parser.add_argument("--experiment", "-e", help='Option for specifying experiment id.', nargs='?')
    parser.add_argument("--tables", "-tb", help='Option for tables names for compare.', nargs='+')
    parser.add_argument('path', help='.py file with collection witch implement CompareCollection interface.'
                                     'This collection will be benchmarked.')
    parser.add_argument('module', help='Name of module with collection for Benchmarking.')
    parser.add_argument('className', help='Name of collection class for Benchmarking.')

    args = parser.parse_args()

    if args.profile:
        if args.profile == ' ':
            with open('config.json', 'r') as file_r:
                parsed = json.load(file_r)
                print(json.dumps(parsed, sort_keys=True, indent=2))
            sys.exit()
        else:
            with open(args.profile, 'r') as file_r, open('config.json', 'w') as file_w:
                parsed = json.load(file_r)
                file_w.write(json.dumps(parsed, sort_keys=True, indent=2))
    if args.template:
        with open(args.template + os.path.sep + 'config_template.json', 'w') as file_w:
            template = {"DBMS1": {"DBMS": "", "ip": "", "port": "", "db": "", "user": "", "password": ""},
                        "DBMS2": {"DBMS": "", "ip": "", "port": "", "db": "", "user": "", "password": ""}}
            file_w.write(json.dumps(template, sort_keys=True, indent=2))
        sys.exit()

    spec = importlib.util.spec_from_file_location(args.module, args.path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cdc = getattr(module, args.className)

    benchmark = Benchmark()
    if args.experiment:
        result = benchmark.testCDCExperimentSet(cdc, args.experiment)
    else:
        if args.tables:
            result = benchmark.testCDCCompareTables(cdc, args.tables)
        else:
            print('Provide table option or experiment option!')
            sys.exit()

    if args.file:
        with open(args.file + os.path.sep + "benchmark_results.json", 'w') as file_w:
            file_w.write(json.dumps(result, sort_keys=True, indent=2))
    else:
        print(json.dumps(result, sort_keys=True, indent=2))
