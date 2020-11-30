import json 
import csv 
from predict import *
fin = open("input.json")
input_data = json.load(fin)

# with open("")
for input_ in input_data:
    if input_["req_type"] == 1:
        # calling predict function:
        """
        output = predict(input_)
        """
        predict(input_)
        pass
    elif input_["req_type"] == 2:
        # calling profile function
        pass
    elif input_["req_type"] == 3:
        # calling match info function 
        pass