#!/usr/bin/python3
import sys

# Reading the file name
previous_node = None

# Opening the v file
v_file = open(sys.argv[1], 'w')

# Reading mapper output
for line in sys.stdin:

    # If line is empty
    if len(line) < 3:
        continue

    # Splitting by tab
    from_node, to_node = line.split(',')
    from_node = from_node.strip()
    to_node = to_node.rstrip()

    # If from node changes
    if from_node != previous_node:

        # Dont print new line for first node
        if previous_node != None:
            print("]")

        print(from_node + "\t[", end = "")

        # Writing to v file
        v_file.write(from_node + ",1\n")

        # Updating node and list
        previous_node = from_node

    else:
        print(",", end = "")

    print(to_node, end = "")
print(']')