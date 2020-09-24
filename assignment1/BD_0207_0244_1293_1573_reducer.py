#!/usr/bin/python3
import sys
import json
#inital counts
c1=0 #number of occurrences of a given word, that has been marked as recognized
c2=0 #number of occurrences of the same word that was not marked as recognized and whose timestamp falls on a weekend
for i in sys.stdin:
	#stripping the line of leading and trailing whitespaces
	line=i.strip()
	#splitting based on tab space
	word,count=i.split('\t')
	#typecasting
	count=int(count)
	#count=1 corresponds to recognized=true
	if count==1:
		c1+=1
	#count=2 corresponds to recognized=false
	if count==2:
		c2+=1
#final output
print(c1)
print(c2)
