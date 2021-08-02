import argparse
import json
import os
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", "-n", help="Name of hello person", nargs='?', const=' ')
    args = parser.parse_args()

    if args.name:
        print("Hellow ", args.name)

if __name__ == '__main__':
    main()
