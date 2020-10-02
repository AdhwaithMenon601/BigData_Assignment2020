#!/usr/bin/python3
import sys
f = open(sys.argv[1], 'r')
d = {}

for line in f:
    key, page_rank = line.strip('\n').split(',')
    d[key] = (float(page_rank), 0)

for i in sys.stdin:
	key,val=i.split("\t")
	val=val.strip("\n")
	#print(val.strip("[").strip("]").split(","))
	l=val.strip("[").strip("]").split(",")
	for i in l:
		if i.strip("\"'").strip(" ").strip("\'").strip("\"") in d.keys():
			print(i.strip("\"'").strip(" ").strip("\'").strip("\"")+"\t"+str(float(d[key][0] / len(l))),sep="")
			temp=d[i.strip("\"'").strip(" ").strip("\'").strip("\"")]
			d[i.strip("\"'").strip(" ").strip("\'").strip("\"")]=(temp[0],1)
no_incoming = [node for node in d.keys() if d[node][1] == 0]
for node in no_incoming:
    print(node.strip("\"'").strip(" ").strip("\'").strip("\"") + "\t0",sep="")
