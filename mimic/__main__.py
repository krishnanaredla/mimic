from __future__ import absolute_import

import os
import sys
import argparse

if sys.path[0] in ("", os.getcwd()):
    sys.path.pop(0)

if __package__ == "":
    path = os.path.dirname(os.path.dirname(__file__))
    sys.path.insert(0, path)

from .main import generateFakeData as fd

parser = argparse.ArgumentParser(description="CLI for generating Fake data")
parser.add_argument("--config", "-c", help="config file location")

parser.add_argument("--output", "-o", help="output location")

args = parser.parse_args()

if __name__ == "__main__":
    sys.exit(fd(args.config, args.output))
