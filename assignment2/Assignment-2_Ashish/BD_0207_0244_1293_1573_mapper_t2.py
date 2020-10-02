#!/usr/bin/python3
import sys

# Opening the adjacency list and page rank file
rank_file = open(sys.argv[1], 'r')
rank_dict = {}

for line in rank_file:

    if (len(line) < 3):
        continue

    # Reading the page rank of the node
    key, page_rank = line.strip('\n').split(',')
    rank_dict[key] = (float(page_rank), 0)

for adj_line in sys.stdin:

    if (len(line) < 3):
        continue

    # Reading the node and its adjacency list
    key_node, node_list = adj_line.strip('\n').split('\t')
    node_list = (node_list[1 : -1]).split(',')

    # Calculating page_rank/n for each node
    for node in node_list:

        # Removing quotes from node key
        node_clean = node.strip("\"'").strip().strip("\'").strip("\"")

        # Checking if it exists in dictionary
        if (node_clean in rank_dict.keys()):

            # Printing to file
            print(node.strip() + "\t" + str(float(rank_dict[key_node][0] / len(node_list))))

            # Updating dictionary
            temp = rank_dict[node_clean]
            rank_dict[node_clean] = (temp[0], 1)

# Printing 0 contribution for the nodes with no incoming links        
no_incoming = [node for node in rank_dict.keys() if rank_dict[node][1] == 0]
for node in no_incoming:

    # Removing quotes from node key
    node_clean = node.strip("\"'").strip().strip("\'").strip("\"")
    print(node_clean + "\t0")
