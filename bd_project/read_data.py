import sys
import json
from pyspark.streaming import StreamingContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row

play_path = "hdfs://localhost:9000/ip/players.csv"
team_path = "hdfs://localhost:9000/ip/teams.csv"


# Creating the streaming spark context and spark session
sp_context = SparkContext('local[3]', "Read_Stream")
ssp_context = StreamingContext(sp_context, 1)
sp_sess = SparkSession.builder.appName('Read_Data').getOrCreate()
sp_context.addFile("/home/nidarshan/bd_project/yo/Data/EPL_stream/stream/duel.py")
from duel import *
# Reading the CSV files using Spark session
players = sp_sess.read.csv(play_path, header=True, inferSchema=True)
teams = sp_sess.read.csv(team_path, header=True, inferSchema=True)

def func(rdd):

    # If RDD is empty, move on
    if rdd.isEmpty():
        print("RDD is empty!")
        return

    # Separating Match and Event JSONs
    match_json = rdd.first()
    event_json = rdd.filter(lambda x : x != match_json).map(lambda x : eval(x))
    event_df = event_json.map(lambda x : Row(**x)).toDF()

    # Calculating the pass accuracy
    #pass_acc = pass_accuracy(event_df)
    print(duel_effectiveness(event_df))
    # Printing details for each JSON
    # event_json.foreach(event_process)

# Connecting to the specified host and port number
data = ssp_context.socketTextStream('localhost', 6100)
data.foreachRDD(func)
ssp_context.start()
ssp_context.awaitTermination()
