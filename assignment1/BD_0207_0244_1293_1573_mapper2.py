#!/usr/bin/python3
# importing required libraries
import sys
import json
import math
# taking word and distance from command line
given_word = sys.argv[1]
distance = float(sys.argv[2])
for data in sys.stdin:
    to_write = True
    # loads data to dictionary
    data = json.loads(data)
    # checking constraints
    # word comprises of only letters and whitespaces
    if not data["word"].replace(" ", "").isalpha():
        to_write = False
    # country code has two capital letters
    if not(data["countrycode"].isalpha() and data["countrycode"].isupper() and len(data["countrycode"]) == 2):
        to_write = False
    # recognized is either true or false
    if not(data["recognized"] == True or data["recognized"] == False):
        to_write = False
    # key_id is numberic string of length 16
    if not(data["key_id"].isdigit() and len(data["key_id"]) == 16):
        to_write = False
    # each stroke has two arrays (x and y) of equal length with length >=1
    if data["drawing"]:
        for i in data["drawing"]:
            if len(i) != 2:
                to_write = False
            elif len(i[0]) != len(i[1]):
                to_write = False
            elif len(i[0])==0:
                to_write = False
    else:
        to_write = False
    # if it passes all constraints
    if to_write == True:
        # taking 0th coordinates of first stroke
        x = data["drawing"][0][0][0]
        y = data["drawing"][0][1][0]
        # euclidien distance
        dis = math.sqrt(x**2+y**2)
        # if the word matches and if calculated 'dis' is greater
        if dis > float(distance) and data["word"] == given_word:
            print(data["countrycode"], "\t", 1,sep="")
