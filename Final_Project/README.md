README FOR FPL PROJECT ANALYSIS
Team: BD_0207_0244_1293_1573
Below is the code for our project on Fantasy Premier League Analysis

The code for executing the project is stored in the folder -
Final_project

FOLDER FILES
The given folder contains three python files-
master.py
metrics.py
ui.py

CODE DESCRIPTION
A description of the three files are as follows - 
1. master.py -> This is the main master file which contains reading data from the stream and calls the required functions to process it from the metrics file. Further this file also saves the required metric outputs onto HDFS and Local System for ease of use.

2. metrics.py -> This file contains the various functions that are used to process the data that is delivered in the stream objects. It additionally also contains the different ML models required by our project.

3. ui.py -> This file contains the UI input of our project. It takes in a JSON object , whose path is provided as an arguement and based on the query , it calls the required functions.


STEPS TO RUN
spark-submit master.py                  (Provided Stream is running on port 6100)

Once the file has finished processing , we then run the required UI
spark-submit ui.py "path to input JSON"

Output is delivered to the required JSON-
output_req_'n'.json
Here 'n' is one of the 3 request types.