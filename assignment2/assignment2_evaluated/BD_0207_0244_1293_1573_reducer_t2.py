#!/usr/bin/python3
import sys
#intial variables
current_word=None
current_sum=0
word=None
#f=open("/home/nidarshan/hadoop/v1","w")
for i in sys.stdin:
	#stripping the line of leading and trailing whitespaces
	i=i.strip()
	#splitting based on tab space
	word,temp_sum=i.split('\t')
	word=word.strip("'\t\n'")
	#typecasting
	temp_sum=float(temp_sum.strip("\t\n"))
	#incrementing count by 1
	if current_word==word:
		current_sum+=temp_sum
	else:
		#if current_word is not None
		if current_word:
			print(current_word+","+"{0:.5f}".format(round(0.15+0.85*current_sum,5)))
			#print(current_word,",",current_count,sep="")
		#setting the word and count
		current_sum=temp_sum
		current_word=word
#accounting for the last word edge case
if current_word==word:
	print(current_word+","+"{0:.5f}".format(round(0.15+0.85*current_sum,5)))
