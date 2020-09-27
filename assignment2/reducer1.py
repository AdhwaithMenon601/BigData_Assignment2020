#!/usr/bin/python3
import sys
path=sys.argv[1]
d={}
d1={}
f=open(path,"w")
for i in sys.stdin:
	key,value=i.strip("\t\n").split(" ")
	if value not in d1:
		f.write(value+","+"1"+"\n")
		d1[value]=1
	if key not in d:
		d[key]=[int(value)]
		f.write(key+","+"1"+"\n")
	else:
		d[key].append(int(value))
f.close()
for i in d:
	print(i,"\t",d[i],sep="")
