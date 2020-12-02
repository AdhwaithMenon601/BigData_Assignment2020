# Assignments and Projects for the Big Data Course, PES University

## Assignments

- Assignment 1 : Implementing Map reduce Tasks using Hadoop 3.0
- Assignment 2 : Page Rank computation using Map Reduce
- Assignment 3 : Introduction to spark using pyspark

## Project : Fantasy Premier League (FPL) Analytics using Streaming Spark
Team: BD_0207_0244_1293_1573

The aim of this project was to perform FPL analytics using Streaming spark to read streamed data and perform various computations for each of the Match Stats streamed.

The code for executing the project is stored in the folder ```Final_Project```

### Files in the Project Foler
The given folder contains three python files-
- master.py
- metrics.py
- ui.py

### Code Description
A description of the three files are as follows - 

1. ```master.py``` : This is the main master file which contains reading data from the stream and calls the required functions to process it from the metrics file. Further this file also saves the required metric outputs onto HDFS and Local System for ease of use.

2. ```metrics.py``` : This file contains the various functions that are used to process the data that is delivered in the stream objects. It additionally also contains the different ML models required by our project.

3. ```ui.py``` : This file contains the UI input of our project. It takes in a JSON object , whose path is provided as an arguement and based on the query , it calls the required functions.


### Step to run the code

1. To run the streaming code (Provided Stream is running on port 6100)
```bash
spark-submit master.py
```

2. Once the file has finished processing , we then run the required UI
```bash
spark-submit ui.py "path to input JSON"
```

3. Output is delivered to the required JSON ```output_req_'n'.json```. Here 'n' is one of the 3 request types.