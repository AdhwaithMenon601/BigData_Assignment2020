#!/usr/bin/python3
#import required libraries
import sys
import json
from datetime import datetime
#taking argument from command line
given_word=sys.argv[1]
for data in sys.stdin:
	to_write=True
	#loads data to dictionary
	data=json.loads(data)
	#checking constraints
	#word comprises of only letters and whitespaces
	if not data["word"].replace(" ","").isalpha():
		to_write=False
	#country code has two capital letters
	if not(data["countrycode"].isalpha() and data["countrycode"].isupper() and len(data["countrycode"])==2):
		to_write=False
	#recognized is either true or false
	if not(data["recognized"]==True or data["recognized"]==False):
		to_write=False
	#key_id is numberic string of length 16
	if not(data["key_id"].isdigit() and len(data["key_id"])==16):
		to_write=False
	#each stroke has two arrays (x and y)
	if data["drawing"]:
		for i in data["drawing"]:
			if len(i)!=2:
				to_write=False
			elif len(i[0])!=len(i[1]):
				to_write=False
			elif len(i[0])==0:
				to_write=False
	else:
		to_write=False
	#if it passes all constraints
	if to_write==True:
		#if the word matches and recognized is true
		if data["word"]==given_word and int(data["recognized"])==1:
			print(data["recognized"],"\t",1)
		#if word matches and recognized is false
		elif data["word"]==given_word and int(data["recognized"])==0:
			#if timestamp corresponds to a weekend
			if datetime.strptime(data["timestamp"][:-4],'%Y-%m-%d %H:%M:%S.%f').weekday()==5 or datetime.strptime(data["timestamp"][:-4],'%Y-%m-%d %H:%M:%S.%f').weekday()==6:
				print(data["recognized"],"\t",2)
			
		
			
	
		
