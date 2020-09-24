#!/usr/bin/python3
import sys
path=sys.argv[1]
f=open(path,"r")
d={}
for i in f:
	key,value=i.strip("\n").split(",")
	d[key]=int(value)
for i in sys.stdin:
	key,value=i.strip("\n").split("\t")
	l=value.strip('[').strip(']').split(",")
	for j in l:
		print(j.strip(" "),"\t",d[key]/len(l),sep="")
