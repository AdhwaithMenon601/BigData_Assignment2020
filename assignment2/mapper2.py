#!/usr/bin/python3
import sys
path=sys.argv[1]
f=open(path,"r")
d={}
for i in f:
	key,value=i.strip("\n").split(",")
	d[key]=[float(value),0]
for i in sys.stdin:
	key,value=i.strip("\n").split("\t")
	l=value.strip('[').strip(']').split(",")
	for j in l:
		if key in d and j in d:
			print(j.strip(" "),"\t",d[key][0]/len(l),sep="")
			d[j.strip(" ")][1]=1
for i in d:
	if d[i][1]==0:
		print(i,"\t",0,sep="")
