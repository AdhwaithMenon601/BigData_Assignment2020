#!/usr/bin/python3
import sys

# Skippin the first 4 lines
for _ in range(4):
    next(sys.stdin)

# Processing remaining lines
for line in sys.stdin:

    # Splitting by tab
    from_node, to_node = line.split('\t')
    to_node = to_node.rstrip()
    output = from_node + "," + to_node

    # Printing to mapper output
    print(output)




