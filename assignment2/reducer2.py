#!/usr/bin/python3
import sys

current_node = None
sum_contrib = 0
key = None

# Reading input
for line in sys.stdin:

    if (len(line) < 3):
        continue

    # Splitting by tab
    key, rank = line.strip('\n').split('\t')
    rank = float(rank)

    # Writing to file
    if key != current_node:

        if (current_node == None):
            current_node = key

        else:
            # Calculating new rank and writing to file
            expr = round(0.15 + (0.85 * sum_contrib), 5)
            print(current_node + ',', end = "")
            print("{0:.5f}".format(expr))

            current_node = key
            sum_contrib = rank

    # Adding contrib
    else:
        sum_contrib += rank

# Printing values for final node
print(current_node + ',' + str(expr), end = "")
