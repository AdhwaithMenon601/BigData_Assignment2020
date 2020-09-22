#python code for mapper 
#converts a given input file into a adjacency list
#! /usr/bin/env python
import sys

#We must ignore first 4 lines
cnt = 0
for line in sys.stdin:
    line = line.strip()
    words = line.split('\t')
    cnt += 1
    if (cnt > 3):
        print('{} {}'.format(words[0],words[0]))