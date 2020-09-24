#!/usr/bin/python3
import sys
path=sys.argv[1]
d={}
f=open(path,"w")
for i in sys.stdin:
	key,value=i.strip("\t\n").split(" ")
	if key not in d:
		d[key]=[int(value)]
		f.write(key+","+"1"+"\n")
	else:
		d[key].append(int(value))
f.close()
for i in d:
	print(i,"\t",d[i],sep="")
