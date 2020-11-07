# Code for pyspark and task1
# Read the dataset and perform operations on it using pyspark
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

# Reading the word and path from command line
word = sys.argv[1]
path2 = sys.argv[2]
path = sys.argv[3]


# Starting a new Spark Session
spark = SparkSession.builder.master("local[*]").appName("task1").getOrCreate()

# Dataset 2 is the shape_stat file uploaded to HDFS
# Next step is to read the CSV file into spark
df1 = spark.read.csv(path, header=True, inferSchema=True)
df1 = df1.withColumn("Total_Strokes", df1["Total_Strokes"].cast(IntegerType()))

# Calculating the averages for the given word
rec_df = df1.filter(df1['recognized'] == "TRUE")
unrec_df = df1.filter(df1['recognized'] == "FALSE")

rec_df = rec_df.filter(rec_df['word'] == word)
unrec_df = unrec_df.filter(unrec_df['word'] == word)

if (not rec_df.head(1) and not unrec_df.head(1)):
    edge1 = "{:.5f}".format(0)
    print(edge1,"\n",edge1,sep='')
else:
    tmp1 = 0
    tmp2 = 0
    if (rec_df.head(1)):
        rec_avg = rec_df.groupBy("word").avg("Total_Strokes")
        tmp2 = rec_avg.select("avg(Total_Strokes)").rdd.flatMap(list).collect()[0]
    if (unrec_df.head(1)):
        unrec_avg = unrec_df.groupBy("word").avg("Total_Strokes")
        tmp1 = unrec_avg.select("avg(Total_Strokes)").rdd.flatMap(list).collect()[0]
    
    
    # Rounding to 5 decimal places
    format_1 = "{:.5f}".format(tmp1)
    format_2 = "{:.5f}".format(tmp2)

    print(format_2,"\n",format_1,sep='')