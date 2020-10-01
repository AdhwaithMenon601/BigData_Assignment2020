#!/usr/bin/python3
import sys
path=sys.argv[1]
d={}
f=open(path,"w")
for i in sys.stdin:
	key,value=i.strip("\t\n").split(" ")
	if key not in d:
		d[key]=[int(value)]
	else:
		d[key].append(int(value))
for i in d:
	print(i,"\t",d[i],sep="")
	f.write(i+","+"1"+"\n")
f.close()
