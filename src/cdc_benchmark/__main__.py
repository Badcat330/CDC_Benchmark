from jschon import JSON, JSONSchema, create_catalog
from cdc_benchmark.benchmark_exceptions import NoConfigFoundException, InvalidConfigException
import argparse
import json
import os
import sys
import random


def validate_test_config() -> dict:
    create_catalog('2020-12', default=True)

    with open("src/cdc_benchmark/data/DBMS_schema.json", "r", encoding="utf-8") as fh:
        DBMS_schema = JSONSchema(json.loads(fh.read()))

    with open("src/cdc_benchmark/data/table_achema.json", "r", encoding="utf-8") as fh:
        table_schema = JSONSchema(json.loads(fh.read()))

    with open("src/cdc_benchmark/data/config_schema.json", "r", encoding="utf-8") as fh:
        config_schema_row = json.loads(fh.read())
        default_config = config_schema_row["examples"][0]
        config_schema = JSONSchema(config_schema_row)


    if os.path.isdir('temporary'):
        with open("temporary/config.json", "r", encoding="utf-8") as fh:
            config_row = json.loads(fh.read())
            config = JSON(config_row)
    else:
        os.mkdir('temporary')
        with open("temporary/config.json", "w", encoding="utf-8") as fh:
            json.dump(default_config, fh, indent=4)
        raise NoConfigFoundException()

    if config_schema.evaluate(config).valid:
        return config_row
    else:
        raise InvalidConfigException()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("workflow_type", nargs=1, choices=['generator', 'test'], help="Type of cdc_benchmark workflow. Can be test or generator.")
    args = parser.parse_args()
    
    if args.workflow_type[0] == 'test':
        print('test')
    elif args.workflow_type[0] == 'generator':
        print('generator')