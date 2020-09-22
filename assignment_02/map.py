#python code for mapper 
#converts a given input file into a adjacency list
#! /usr/bin/env python
import sys

for line in sys.stdin:
    line = line.strip()
    words = line.split('\t')