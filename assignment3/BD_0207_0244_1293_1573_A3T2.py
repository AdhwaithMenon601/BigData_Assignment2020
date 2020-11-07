# Code for pyspark and task2
# Read the dataset and perform operations on it using pyspark
"""
    The current code is just joining the 2 input files
    Please implement a code for using Co-partitioning as it is required for final report
"""
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc

# Reading the different keyword arugements
myword = sys.argv[1]
k = sys.argv[2]
path1 = sys.argv[3]
path2 = sys.argv[4]

# In the following , path1 is for Dataset1 and path2 is for Dataset2
# Dataset2 in my case is shape_stat.csv and Dataset1 in my case is shape.csv

# Building the spark session
spark = SparkSession.builder.master("local[*]").appName("task1").getOrCreate()

# Reading from the data
df1 = spark.read.csv(path1, header=True, inferSchema=True)
df2 = spark.read.csv(path2, header=True, inferSchema=True)
df2 = df2.withColumn("Total_Strokes", df2["Total_Strokes"].cast(IntegerType()))

# Joining the datasets using spark in built method - 'join' 
# I am currently using an inner join
df = df1.join(df2, on=["key_id"], how='inner').drop(df1['word'])

# First filter via 'unrecognised'
df = df.filter(df["recognized"] == "FALSE")

# Then filter via number of strokes less than 'k'
df = df.filter(df["Total_Strokes"] < k)

# Then filter via 'word'
df = df.filter(df["word"] == myword)

# Post this groupBy the key('country_id') and aggregate via the 'count' function
if (not df.head(1)):
    print(0)
else:
    grouped_df = df.groupBy("countrycode").count()
    grouped_df = grouped_df.orderBy("countrycode")

    # Finally displaying it via select
    final_ar = grouped_df.select("countrycode","count").rdd.flatMap(list).collect()

    # Printing out the list
    # Additionally shape could also be changed
    for i in range(0,len(final_ar),2):
        output_str = final_ar[i] + ',' + str(final_ar[i+1])
        print(output_str)