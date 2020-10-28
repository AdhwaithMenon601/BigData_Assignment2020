# Code for pyspark and task1
# Read the dataset and perform operations on it using pyspark
import sys
import pandas as pd
import numpy as np
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

# Reading the word and path from command line
word = sys.argv[1]
path = sys.argv[2]
# The below is only for final submission
# path2 = sys.argv[3]

# Starting a new Spark Session
spark = SparkSession.builder.master("local[*]").appName("task1").getOrCreate()

# Dataset 2 is the shape_stat file uploaded to HDFS
# Next step is to read the CSV file into spark
df1 = spark.read.csv(path, header=True, inferSchema=True)
df1 = df1.withColumn("Total_Strokes", df1["Total_Strokes"].cast(IntegerType()))

# Calculating the averages for the given word
rec_df = df1.filter(df1['reccognized'] == "True")
unrec_df = df1.filter(df1['reccognized'] == "False")

rec_df = rec_df.filter(rec_df['word'] == word)
unrec_df = unrec_df.filter(unrec_df['word'] == word)

if (not rec_df.head(1) or not unrec_df.head(1)):
    print("Empty")
else:
    rec_avg = rec_df.groupBy("word").avg("Total_Strokes")
    unrec_avg = unrec_df.groupBy("word").avg("Total_Strokes")
    # Printing the respective averages
    
    tmp1 = unrec_avg.select("avg(Total_Strokes)").rdd.flatMap(list).collect()[0]
    tmp2 = rec_avg.select("avg(Total_Strokes)").rdd.flatMap(list).collect()[0]
    
    print(tmp1,"\n",tmp2,sep='')
