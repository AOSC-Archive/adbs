#!/usr/bin/env python3
"""
ADBS - Autobuild Database-based Build System
"""

import os
import sys
import shutil
import argparse

import adbs
from adbs import builder


def main():
    parser = argparse.ArgumentParser(
        description='ADBS - Autobuild Database-based Build System\n'
        'An automatic build system for AOSC packages.')
    parser.add_argument('--version',
                        help='Show the version and exit', action="version",
                        version='ACBS version {}'.format(adbs.__version__))
    parser.add_argument('-v', '--verbose',
                        help='Show debug logs', action="store_true")
    parser.add_argument('-c', '--config',
                        help='Specify the config file. Command line options will overwrite the ones in config file.')
    parser.add_argument('-t', '--tree',
                        help='Specify path to the abbs tree')
    parser.add_argument('-l', '--log-dir',
                        help='Directory for log files')
    parser.add_argument('-c', '--cache-dir',
                        help='Directory for cache and downloaded files')
    parser.add_argument('-b', '--build-dir',
                        help='Directory for build process files')
    parser.add_argument('-B', '--build-clean', action='store_true',
                        help='Clear build directory')
    parser.add_argument('-k', '--keep', action='store_true',
                        help='Keep build directory even if build successfully.')
    parser.add_argument('packages', nargs='*', help='Packages or groups to be built')
    args = parser.parse_args()
    ...


if __name__ == '__main__':
    main()
