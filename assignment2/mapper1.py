#!/usr/bin/python3
import sys
for i in sys.stdin:
	if i[0]=='#':
		continue;
	key,value=i.strip("\n").split("\t")
	print(key," ",value,sep="")
