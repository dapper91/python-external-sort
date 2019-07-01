"""
External sort.
"""

import argparse
import logging

import ext_sort

LOGGER_FORMAT = '[%(levelname)-8s] %(asctime)-15s (%(name)s): %(message)s'


parser = argparse.ArgumentParser(description='External sort.')
parser.add_argument('-l', '--loglevel', dest='loglevel', choices=['debug', 'info', 'warning', 'error'],
                    default='info', help='logging level')
parser.add_argument('-i', '--infile', dest='infile', required=True, help='input file to be sorted')
parser.add_argument('-o', '--outfile', dest='outfile', required=True, help='file the result will be saved in')
parser.add_argument('-b', '--chunk_size', dest='chunk_size', type=int, required=True,
                    help='number of elements that will be sorted in the main memory')
parser.add_argument('-m', '--chunk_mem', dest='chunk_mem', type=int,
                    help='max memory size that will consumed by one worker')
parser.add_argument('-t', '--total_mem', dest='total_mem', type=int,
                    help='max total memory size that will consumed by all the workers')
parser.add_argument('-w', '--workers', dest='workers', type=int,
                    help='number of workers sorting will be performed in (default: number of cpu cores)')
parser.add_argument('--tmp_dir', dest='tmp_dir', help='directory temporary files will be created in')
parser.add_argument('-v', '--version', action='version', version=ext_sort.__version__)

args = parser.parse_args()

logging.basicConfig(level=getattr(logging, args.loglevel.upper()), format=LOGGER_FORMAT)


with open(args.infile, 'rb') as reader, open(args.outfile, 'wb') as writer:
    ext_sort.sort(
        reader, writer,
        chunk_size=args.chunk_size,
        chunk_mem=args.chunk_mem,
        total_mem=args.total_mem,
        workers_cnt=args.workers,
        tmp_dir=args.tmp_dir,
    )
