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
sp_context.addFile("calc_stats_1.py")
sp_context.addFile("calc_stats_2.py")
sp_context.addFile("init_stats.py")
from calc_stats_1 import *
from calc_stats_2 import *
from init_stats import *
# Reading the CSV files using Spark session
players = sp_sess.read.csv(play_path, header=True, inferSchema=True)
teams = sp_sess.read.csv(team_path, header=True, inferSchema=True)

# Dictionary for the player chemistry and rating
# Might have to make it into a dataframe if space is too large
player_chemistry = {}
player_rating = {}
prev_player_rating = {}

def func(rdd):

    # If RDD is empty, move on
    if rdd.isEmpty():
        print("RDD is empty!")
        return

    # Separating Match and Event JSONs
    match_json = rdd.first()
    event_json = rdd.filter(lambda x : x != match_json).map(lambda x : eval(x))
    event_df = event_json.map(lambda x : Row(**x)).toDF()

    match_json = rdd.first()
    match_df = rdd.filter(lambda x : x == match_json).map(lambda x : eval(x))
    match_df = match_df.map(lambda x : Row(**x)).toDF()

    # Store the previous player ratings
    prev_player_rating = player_rating.copy()

    # Calculating the metrics
    '''
    print(pass_accuracy(event_df))
    print(duel_effectiveness(event_df))
    print(freekick_effectiveness(event_df))
    print(shots_effectiveness(event_df))
    fouls_per_player = fouls_loss(event_df)    # Required this for player rating
    print(fouls_per_player)
    print(own_goal(event_df))
    player_rating(player_rating, player_contribution, own_per_player, fouls_per_player)
    calc_chemistry(player_chemistry, player_rating, prev_player_rating, team_player_dict)
    print(player_chemistry)
    '''

# Connecting to the specified host and port number
data = ssp_context.socketTextStream('localhost', 6100)
player_chemistry = init_chemistry(players)
player_rating = init_ratings(players)
data.foreachRDD(func)
ssp_context.start()
ssp_context.awaitTermination()
