import sys
import json
from pyspark.streaming import StreamingContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row

play_path = "hdfs://localhost:9000/players.csv"
team_path = "hdfs://localhost:9000/teams.csv"


# Creating the streaming spark context and spark session
sp_context = SparkContext('local[2]', "Read_Stream")
ssp_context = StreamingContext(sp_context, 5)
sp_sess = SparkSession.builder.appName('Read_Data').getOrCreate()
sp_context.addFile("duel.py")
sp_context.addFile("free_kick.py")
sp_context.addFile("fouls.py")
sp_context.addFile("owngoal.py")
from duel import *
from free_kick import *
from fouls import *
from owngoal import *
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
    print(free_kick_calc(event_df))
    num_fouls = fouls_loss(event_df)    # Required this for player rating
    print(num_fouls)
    print(own_goal_calc(event_df))
    # Printing details for each JSON
    # event_json.foreach(event_process)

# Connecting to the specified host and port number
data = ssp_context.socketTextStream('localhost', 6100)
data.foreachRDD(func)
ssp_context.start()
ssp_context.awaitTermination()
