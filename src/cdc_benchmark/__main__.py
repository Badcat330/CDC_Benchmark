from cdc_benchmark.benchmark import Benchmark
from jschon import JSON, JSONSchema, create_catalog
from cdc_benchmark.benchmark_exceptions import NoConfigFoundException, InvalidConfigException
from cdc_benchmark.generator import CDCGenerator
import argparse
import json
import os
import subprocess


def validate_test_config() -> dict:
    create_catalog('2020-12', default=True)

    with open("src/cdc_benchmark/data/DBMS_schema.json", "r", encoding="utf-8") as fh:
        JSONSchema(json.loads(fh.read()))

    with open("src/cdc_benchmark/data/table_schema.json", "r", encoding="utf-8") as fh:
        JSONSchema(json.loads(fh.read()))

    with open("src/cdc_benchmark/data/config_schema.json", "r", encoding="utf-8") as fh:
        config_schema_row = json.loads(fh.read())
        default_config = config_schema_row["examples"][0]
        config_schema = JSONSchema(config_schema_row)

    if os.path.isdir('temporary'):
        with open("temporary/test_config.json", "r", encoding="utf-8") as fh:
            config_row = json.loads(fh.read())
            config = JSON(config_row)
    else:
        os.mkdir('temporary')
        with open("temporary/test_config.json", "w", encoding="utf-8") as fh:
            json.dump(default_config, fh, indent=4)
        raise NoConfigFoundException()

    if config_schema.evaluate(config).valid:
        return config_row
    else:
        raise InvalidConfigException()


def validate_generator_config():
    pass


def open_config(name):
    if os.path.isdir('temporary'):
        subprocess.run(["vim", f"temporary/{name}_config.json"])
    else:
        os.mkdir('temporary')
        with open("src/cdc_benchmark/data/config_schema.json", "r", encoding="utf-8") as fh:
            config_schema_row = json.loads(fh.read())
            default_config = config_schema_row["examples"][0]

        with open(f"temporary/{name}_config.json", "w", encoding="utf-8") as fh:
            json.dump(default_config, fh, indent=4)
        subprocess.run(["vim", f"temporary/{name}_config.json"])


def give_config(name, path):
    if not os.path.isdir('temporary'):
        os.mkdir('temporary')

    with open(path, "r", encoding="utf-8") as fh:
        config_row = json.loads(fh.read())
        config = config_row

    with open(f"temporary/{name}_config.json", "w", encoding="utf-8") as fh:
        json.dump(config, fh, indent=4)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("workflow_type", nargs=1, choices=['generator', 'test'],
                        help="Type of cdc_benchmark workflow. Can be test or generator.")
    parser.add_argument("--edit", '-e', action='store_const', const=True, required=False,
                        help="Flag for editing config.")
    parser.add_argument("--params", '-p', nargs=1, required=False, help="Give Path for existing config.")
    parser.add_argument("--file", '-f', nargs=1, required=False, help="Give Path for saving output to file.")
    parser.add_argument("--delete", '-d', action='store_const', const=True, required=False,
                        help="Flag for deleting experiment.")
    parser.add_argument("--change", '-c', nargs=4, required=False, help="Make changes in table.")

    args = parser.parse_args()

    if args.params:
        give_config(args.workflow_type[0], args.params[0])

    if args.edit and not args.params:
        open_config(args.workflow_type[0])
    elif args.workflow_type[0] == 'test':
        print("Validate config")
        # TODO: Handle exceptions
        config = validate_test_config()
        print("Config valid! Start testing!")
        benchmark = Benchmark(config)
        result = benchmark.start_benchmarking()
        print("Finish benchmarking!")
        if args.file:
            name = "benchmark_result_" + benchmark.name + ".json"
            with open(os.path.join(args.file[0], name), "w", encoding="utf-8") as fh:
                json.dump(result, fh, indent=4)
        else:
            print(json.dumps(result, indent=4))
    elif args.workflow_type[0] == 'generator':
        gen = CDCGenerator()
        if args.delete:
            gen.exp_delete()
        elif args.change:
            gen.make_rows_changes(atable=args.change[0],
                                  ucount=int(args.change[1]),
                                  icount=int(args.change[2]),
                                  dcount=int(args.change[3]))
        else:
            gen.exp_create()
