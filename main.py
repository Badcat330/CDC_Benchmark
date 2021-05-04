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
    parser.add_argument('path', help='.py file with collection witch implement CompareCollection interface.'
                                     'This collection will be benchmarked.')
    parser.add_argument('module', help='Name of module with collection for Benchmarking.')
    parser.add_argument('className', help='Name of collection class for Benchmarking.')

    args = parser.parse_args()

    if args.profile:
        if args.profile == ' ':
            with open('config.json', 'r') as file_r:
                parsed = json.load(file_r)
                print(json.dumps(parsed, sort_keys=True, indent=4))
        else:
            with open(args.profile, 'r') as file_r, open('config.json', 'w') as file_w:
                parsed = json.load(file_r)
                file_w.write(json.dumps(parsed, sort_keys=True, indent=4))
    if args.template:
        with open(args.template + os.path.sep + 'config_template.json', 'w') as file_w:
            template = {"ip": "", "port": "", "db": "", "login": "", "password": ""}
            file_w.write(json.dumps(template, sort_keys=True, indent=4))

    spec = importlib.util.spec_from_file_location(args.module, args.path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cdc = getattr(module, args.className)

    benchmark = Benchmark()
    result = benchmark.testCDC(cdc())

    for line in result:
        print(line, '=', result[line])
