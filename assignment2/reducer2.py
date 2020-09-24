#!/usr/bin/python3
import sys
#intial variables
current_word=None
current_sum=0
word=None
f=open("/home/nidarshan/a2/v1.txt","w")
for i in sys.stdin:
	#stripping the line of leading and trailing whitespaces
	line=i.strip()
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
			f.write(current_word+","+str(0.15+0.85*current_sum)+"\n")
			#print(current_word,",",current_count,sep="")
		#setting the word and count
		current_sum=temp_sum
		current_word=word
#accounting for the last word edge case
if current_word==word:
	f.write(current_word+","+str(0.15+0.85*current_sum)+"\n")
f.close()
