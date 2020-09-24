#!/usr/bin/python3
import sys
#intial variables
current_word=None
current_count=0
word=None
for i in sys.stdin:
	#stripping the line of leading and trailing whitespaces
	line=i.strip()
	#splitting based on tab space
	word,count=i.split('\t')
	#typecasting
	count=int(count)
	#incrementing count by 1
	if current_word==word:
		current_count+=count
	else:
		#if current_word is not None
		if current_word:
			print(current_word,",",current_count,sep="")
		#setting the word and count
		current_count=count
		current_word=word
#accounting for the last word edge case
if current_word==word:
	print(current_word,",",current_count,sep="")
