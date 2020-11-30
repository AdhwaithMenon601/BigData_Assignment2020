import sys
import json
from pyspark.streaming import StreamingContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col

# Setting the paths of the CSV files
play_path = "hdfs://localhost:9000/players.csv"
team_path = "hdfs://localhost:9000/teams.csv"

# Dictionary for the player chemistry and rating
# Might have to make it into a dataframe if space is too large
player_chemistry = {}
player_ratings = {}
profile_schema = None
player_profile = None
regr_player = None

def init_regr():
    regr_schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('birth', StringType(), True),
        StructField('cur', StringType(), True),
        StructField('rating', FloatType(), True),
        StructField('diff', IntegerType(), True)
    ])

    new_df = sp_sess.createDataFrame(sp_sess.sparkContext.emptyRDD(), regr_schema)

    return new_df

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
        StructField('ShotsOnTarget', IntegerType(), True),
        StructField('Matches', IntegerType(), True)
    ])

    # Creating the empty dataframe
    player_profile = {}

    return player_profile

# Function for player profile
def get_profile(player_profile, fouls_per_player, own_per_player, goals_per_player, pass_ac, shots_eff, teams_dict):
    # Create a new row in the dataframe and check if player id exists
    # If the player id does not exist , simply append and add
    # Else we must remove that row and join with other dataframe
    # unique_teams = teams_dict.keys()
    players = sp_sess.read.csv(play_path, header=True, inferSchema=True)

    for i in teams_dict:
        for player_id in teams_dict[i]:
            # Checking for value in the dictionary
            if (player_id not in fouls_per_player):
                fouls_per_player.update({player_id:0})
            
            if (player_id not in own_per_player):
                own_per_player.update({player_id:0})
            
            if (player_id not in pass_ac):
                pass_ac.update({player_id:0})
            
            if (player_id not in shots_eff):
                shots_eff.update({player_id:0})
            
            '''
            if (player_id not in goals_per_player):
                fouls_per_player.update({player_id:0})
            '''

            name_df = players.filter(players['Id'] == player_id)
            player_name = name_df.select("name").collect()[0].name
            
            matches = 0
            if (player_id not in player_profile):
                matches = 1
            else:
                matches = player_profile[player_id][6] + 1


            new_list = [int(fouls_per_player[player_id]), player_name, int(goals_per_player), int(own_per_player[player_id]), pass_ac[player_id], int(shots_eff[player_id]), matches]
            player_profile.update({player_id : new_list})

    return player_profile

# For Linear regression , preparing dataframe
def linreg_predict(player_profile, player_ratings, cur_date, teams_dict, regr_player):
    players = sp_sess.read.csv(play_path, header=True, inferSchema=True)

    player_reg_list = []
    player_l = []

    r_schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('birth', StringType(), True),
        StructField('cur', StringType(), True),
        StructField('rating', FloatType(), True),
        StructField('diff', IntegerType(), True)
    ])

    for j in teams_dict:
        for i in teams_dict[j]:
            player_reg = {}
            player_l.append(i)
            player_reg["id"] = i
            name_df = players.filter(players['Id'] == i)
            player_date = name_df.select("birthDate").collect()[0].birthDate
            player_reg["birth"] = player_date
            player_reg["cur"] = cur_date
            if (i not in player_ratings):
                player_ratings.update({i:0})
            player_reg["rating"] = player_ratings[i]
            player_reg_list.append(player_reg)
    
    player_reg = sp_sess.createDataFrame(player_reg_list, r_schema)
    temp_df = regr_player.filter(~col("id").isin(player_l))
    
    new_reg = player_reg.withColumn("diff",datediff(col("cur"),col("birth")))

    if (not(temp_df.head(1))):
        final_df = new_reg
    else:
        final_df = temp_df.union(new_reg)

    return final_df

def process_record(rdd):

    # If RDD is empty, move on
    if rdd.isEmpty():
        print("RDD is empty!")
        return

    # Separating Match and Event JSONs
    match_json = rdd.first()
    event_json = rdd.filter(lambda x : x != match_json).map(lambda x : eval(x))
    event_df = event_json.map(lambda x : Row(**x)).toDF()

    match_json = eval(rdd.first())
    cur_date = match_json["dateutc"]
    print(cur_date)

    # match_df = rdd.filter(lambda x : x == match_json).map(lambda x : eval(x))
    # match_df = match_df.map(lambda x : Row(**x)).toDF()


    # Need to use global variable in this case
    global player_profile 
    global player_ratings
    global player_chemistry
    global regr_player

    # To insert into the player profile dataframe , we need to create a new df
    # And union this with the player profile
    """
    new_row = [(1134,'abc', 5, 6, 7, 0.56, 0)]
    new_df = sp_sess.createDataFrame(new_row, profile_schema)
    player_profile = player_profile.union(new_df)
    """

    # Store the previous player ratings
    prev_player_rating = player_ratings.copy()

    # Calling ret_players
    teams_dict = ret_players(match_json)

    # Calculating the metrics
    pass_ac = pass_accuracy(event_df)
    duel_eff = duel_effectiveness(event_df)
    free_eff = freekick_effectiveness(event_df)
    shots_eff = shots_effectiveness(event_df)
    fouls_per_player = fouls_loss(event_df)    
    own_per_player = own_goal(event_df)
    player_contribution = player_contribution_main(match_json, pass_ac, duel_eff, free_eff, shots_eff)
    player_ratings = player_rating(player_ratings, player_contribution, own_per_player, fouls_per_player)
    player_chemistry = calc_chemistry(player_chemistry, player_ratings, prev_player_rating, teams_dict)
    player_profile = get_profile(player_profile, fouls_per_player, own_per_player, 3, pass_ac, shots_eff, teams_dict)
    regr_player = linreg_predict(player_profile, player_ratings, cur_date , teams_dict, regr_player)
    regr_player.show()
    """
    for i in player_profile:
        print(player_profile[i])
    """
    """for it in player_chemistry:
        print("{0:.5f}".format(player_chemistry[it]))"""
    # It is getting updated , but few only are due to the massive size of the pairs dataset

if __name__ == '__main__':

    # Creating the streaming spark context and spark session
    sp_context = SparkContext('local[2]', "Read_Stream")
    ssp_context = StreamingContext(sp_context, 5.000)
    sp_sess = SparkSession.builder.appName('Read_Data').getOrCreate()

    # Adding the files to Spark Context
    sp_context.addFile("calc_stats_1.py")
    sp_context.addFile("calc_stats_2.py")
    sp_context.addFile("init_stats.py")

    # Reading the CSV files using Spark session
    players = sp_sess.read.csv(play_path, header=True, inferSchema=True)
    teams = sp_sess.read.csv(team_path, header=True, inferSchema=True)

    # Importing the files
    from calc_stats_1 import *
    from calc_stats_2 import *
    from init_stats import *

    # Initializing the player chem and ratings
    player_chemistry = init_chemistry(players)
    player_ratings = init_ratings(players)
    regr_player = init_regr()

    # Adds player profile
    player_profile = init_profile()
    # player_profile.show()

    # Connecting to the specified host and port number
    data = ssp_context.socketTextStream('localhost', 6100)
    data.foreachRDD(process_record)
    ssp_context.start()
    ssp_context.awaitTermination()
