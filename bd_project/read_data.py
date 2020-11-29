import sys
import json
from pyspark.streaming import StreamingContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *

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
profile_schema = None
player_profile = None

# Initialises the schema
def init_profile():
    """
        init_profile : Initalises the player profile

        Returns :
        player_profile {df} -- Dataframe of profile
    """
    profile_schema = StructType([
        StructField('PlayerId', IntegerType(), True),
        StructField('Name', StringType(), True),
        StructField('Fouls', IntegerType(), True),
        StructField('Goals', IntegerType(), True),
        StructField('Own Goals', IntegerType(), True),
        StructField('PassAccuracy', FloatType(), True),
        StructField('ShotsOnTarget', IntegerType(), True)
    ])

    # Creating the empty dataframe
    player_profile = sp_sess.createDataFrame(sp_context.emptyRDD(), profile_schema)

    return player_profile


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


    # Need to use global variable in this case
    global player_profile 
    global player_rating
    global player_chemistry

    # To insert into the player profile dataframe , we need to create a new df
    # And union this with the player profile
    """
    new_row = [(1134,'abc', 5, 6, 7, 0.56, 0)]
    new_df = sp_sess.createDataFrame(new_row, profile_schema)
    player_profile = player_profile.union(new_df)
    """

    # Store the previous player ratings
    prev_player_rating = player_rating.copy()

    # Calculating the metrics
    pass_ac = pass_accuracy(event_df)
    duel_eff = duel_effectiveness(event_df)
    free_eff = freekick_effectiveness(event_df)
    shots_eff = shots_effectiveness(event_df)
    fouls_per_player = fouls_loss(event_df)    
    own_per_player = own_goal(event_df))
    team_player_dict = ret_players(match_df)
    player_contribution = player_contribution_main(match_df, pass_ac, duel_eff, free_eff, shots_eff)
    player_rating(player_rating, player_contribution, own_per_player, fouls_per_player)
    calc_chemistry(player_chemistry, player_rating, prev_player_rating, team_player_dict)

player_chemistry = init_chemistry(players)
player_rating = init_ratings(players)
# Adds player profile
player_profile = init_profile()
player_profile.show()

# Connecting to the specified host and port number
data = ssp_context.socketTextStream('localhost', 6100)
data.foreachRDD(func)
ssp_context.start()
ssp_context.awaitTermination()
