#!/usr/bin/python
import sys
import os
import json
import random
import requests
from pyspark.streaming import StreamingContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col

# Setting the paths of the CSV files
play_path = "hdfs://localhost:9000/players.csv"
team_path = "hdfs://localhost:9000/teams.csv"

players = None
teams = None

# Dictionary for the player chemistry and rating
# Might have to make it into a dataframe if space is too large
player_chemistry = {}
player_ratings = {}
profile_schema = None
player_profile = None
regr_player = None
match_details = []

# Globals for records
teams = None
players = None
match_count = 0
check_count = 1
event_rows = []

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

def filter_players(teams_dict, players_list):
    """
    filter_players : Filtering players not in CSV

    Arguments:
        teams_dict {dict} -- Dict containing player IDs
        players_list {list} -- Players list for validating

    Returns:
        dict -- Dictionary containing the valid players
    """

    for key in teams_dict:

        # Going over the match players
        id_list = teams_dict[key]
        for item in id_list:

            # Checking if match player doesnt exist
            if item not in players_list:
                id_list.remove(item)

        # Reassigning value to dict
        teams_dict.update({key: id_list})

    return teams_dict

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
            
            if (player_id not in goals_per_player):
                goals_per_player.update({player_id:0})

            name_df = players.filter(players['Id'] == player_id)
            player_name = name_df.select("name").collect()[0].name
            
            matches = 0
            if (player_id not in player_profile):
                matches = 1
            else:
                matches = player_profile[player_id][6] + 1


            new_list = [int(fouls_per_player[player_id]), player_name, int(goals_per_player[player_id]), int(own_per_player[player_id]), pass_ac[player_id], shots_eff[player_id], matches]
            player_profile.update({player_id : new_list})

    return player_profile

# For Linear regression , preparing dataframe
def linreg_predict(player_profile, player_ratings, cur_date, teams_dict, regr_player):
    """
    linreg_predict : Returns a Dataframe for the Regression model

    Arguments:
        player_profile {dict} -- Dict for player profile
        player_ratings {dict} -- Dict for player ratings
        cur_date {date obj} -- Date of the current match
        teams_dict {dict} -- Dict containing players in each match

    Returns:
        Pyspark dataframe -- Dataframe for regression
    """

    player_reg_list = []

    # Schema for the dataframe
    r_schema = StructType([
        StructField('rating', FloatType(), True),
        StructField('diff', IntegerType(), True)
    ])

    for j in teams_dict:
        for i in teams_dict[j]:
            player_reg = {}

            # Finding player birthdary in the CSV
            name_df = players.filter(players['Id'] == i)
            player_date = name_df.select("birthDate").collect()[0].birthDate
            cur_date = str(cur_date)[:10]

            # Using datetime for finding difference in dates
            player_date = datetime.datetime.strptime(player_date, r"%Y-%m-%d")
            cur_date = datetime.datetime.strptime(cur_date, r"%Y-%m-%d")
            player_reg['diff'] = int(abs((cur_date - player_date).days))

            # If player doesnt exist in ratings
            if (i not in player_ratings):
                player_ratings.update({i: 0})

            # Updating player rating in dict
            player_reg["rating"] = player_ratings[i]
            player_reg_list.append(player_reg)

    # Creating the dataframe for regression
    player_reg = sp_sess.createDataFrame(player_reg_list, r_schema)
    return player_reg


# Code for finding match data
def fill_match_info(match_details, match_df, teams_dict):
    players = sp_sess.read.csv(play_path, header=True, inferSchema=True)
    teams = sp_sess.read.csv(team_path, header=True, inferSchema=True)
    req_date = match_df["dateutc"]
    winner = match_df["winner"]
    gameweek = match_df["gameweek"]
    duration = match_df["duration"]
    venue = match_df["venue"]

    team_id = list(match_df['teamsData'].keys())
    one_df = teams.filter(teams['Id'] == team_id[0])
    two_df = teams.filter(teams['Id'] == team_id[1])

    t1 = one_df.select("name").collect()[0].name
    t2 = two_df.select("name").collect()[0].name

    teams_compete = t1 + '-' + t2
    
    bench_1 = match_df['teamsData'][team_id[0]]['formation']['bench']
    bench_2 = match_df['teamsData'][team_id[1]]['formation']['bench']
    lineup_1 = match_df['teamsData'][team_id[0]]['formation']['lineup']
    lineup_2 = match_df['teamsData'][team_id[1]]['formation']['lineup']

    own_goals = []
    goals = []
    red_cards = []
    yellow_cards = []
    # For lineup of team 1
    for i in lineup_1:
        player_id = i['playerId']
        name_df = players.filter(players['Id'] == player_id)
        if (not(name_df.head(1))):
            continue
        player_name = name_df.select("name").collect()[0].name
        if ('ownGoals' in i and (i['ownGoals'] != "null")):
            if(int(i['ownGoals']) > 0):
                own_goals.append({"name":player_name, "team":t1, "number_of_goals":i['ownGoals']})
        if ('goals' in i and (i['goals'] != "null")):
            if(i['goals'].isdigit() and int(i['goals']) > 0):
                goals.append({"name":player_name, "team":t1, "number_of_goals":i['goals']})
        if ('yellowCards'in i and (i['yellowCards'] != "null")):
            if(int(i['yellowCards']) > 0):
                yellow_cards.append({"name":player_name, "team":t1})
        if ('redCards'in i and (i['redCards'] != "null")):
            if(int(i['redCards']) > 0):
                red_cards.append({"name":player_name, "team":t1})
    
    # For bench of team 1
    for i in bench_1:
        player_id = i['playerId']
        name_df = players.filter(players['Id'] == player_id)
        if (not(name_df.head(1))):
            continue
        player_name = name_df.select("name").collect()[0].name
        if ('ownGoals' in i and (i['ownGoals'] != "null")):
            if(int(i['ownGoals']) > 0):
                own_goals.append({"name":player_name, "team":t1, "number_of_goals":i['ownGoals']})
        if ('goals' in i and (i['goals'] != "null")):
            if(int(i['goals']) > 0):
                goals.append({"name":player_name, "team":t1, "number_of_goals":i['goals']})
        if ('yellowCards'in i and (i['yellowCards'] != "null")):
            if(int(i['yellowCards']) > 0):
                yellow_cards.append({"name":player_name, "team":t1})
        if ('redCards'in i and (i['redCards'] != "null")):
            if(int(i['redCards']) > 0):
                red_cards.append({"name":player_name, "team":t1})
    
    # Formation for lineup 2
    for i in lineup_2:
        player_id = i['playerId']
        name_df = players.filter(players['Id'] == player_id)
        if (not(name_df.head(1))):
            continue
        player_name = name_df.select("name").collect()[0].name
        if ('ownGoals' in i and (i['ownGoals']!= "null")):
            if(int(i['ownGoals']) > 0):
                own_goals.append({"name":player_name, "team":t2, "number_of_goals":i['ownGoals']})
        if ('goals' in i and (i['goals'] != "null")):
            if(int(i['goals']) > 0):
                goals.append({"name":player_name, "team":t2, "number_of_goals":i['goals']})
        if ('yellowCards'in i and (i['yellowCards'] != "null")):
            if(int(i['yellowCards']) > 0):
                yellow_cards.append({"name":player_name, "team":t2})
        if ('redCards'in i and (i['redCards'] != "null")):
            if(int(i['redCards']) > 0):
                red_cards.append({"name":player_name, "team":t2})
    
    # Formation for bench 2
    for i in bench_2:
        player_id = i['playerId']
        name_df = players.filter(players['Id'] == player_id)
        if (not(name_df.head(1))):
            continue
        player_name = name_df.select("name").collect()[0].name
        if ('ownGoals' in i and (i['ownGoals'] != "null")):
            if(int(i['ownGoals']) > 0):
                own_goals.append({"name":player_name, "team":t2, "number_of_goals":i['ownGoals']})
        if ('goals' in i and (i['goals'] != "null")):
            if(int(i['goals']) > 0):
                goals.append({"name":player_name, "team":t2, "number_of_goals":i['goals']})
        if ('yellowCards'in i and (i['yellowCards'] != "null")):
            if(int(i['yellowCards']) > 0):
                yellow_cards.append({"name":player_name, "team":t2})
        if ('redCards'in i and (i['redCards'] != "null")):
            if(int(i['redCards']) > 0):
                red_cards.append({"name":player_name, "team":t2})

    new_match_data = {"date":req_date, "duration":duration, "winner":winner,\
         "venue":venue, "gameweek":gameweek, "teams_playing":teams_compete,\
              "goals":goals, "own_goals":own_goals,"yellow_cards":yellow_cards,\
                   "red_cards":red_cards}

    
    match_details.append(new_match_data)

    return match_details

def match_data():

    # Need to use global variable in this case
    global player_profile
    global event_rows
    global player_ratings
    global player_chemistry
    global match_count
    global players
    global teams
    global regr_player
    global match_details
    global check_count

    # If only match data is present
    if (len(event_rows) == 1):

        # Creating random data for the model
        cols = ['diff', 'rating']
        rand_data = []

        # Generating random data
        for i in range(10):
            random.seed(i)
            tuple_dat = (random.random(), (random.random() * 10))
            rand_data.append(tuple_dat)

        # Creating the dataframe
        rand_df = sp_sess.createDataFrame(data=rand_data, schema=cols)

        # Initializing the model
        make_model(rand_df, True)
        return

    # Separating the events and match data
    match_df = event_rows[0]
    events_list = event_rows[1: -1]
    
    # Writing to the match JSON
    with open("match_details.json","a") as file:
        if (check_count < 4):
            file.write(json.dumps(match_df, indent=4) + ',')
            check_count += 1
        else:
            file.write(json.dumps(match_df, indent=4))
            check_count += 1

    for i in range(len(events_list)):
        sub_event = events_list[i]['subEventId']
        time = events_list[i]['eventSec']

        # Fixing subEventID
        if sub_event == '':
            sub_event = 0

        # Updating in dict
        events_list[i].update({'subEventId': int(sub_event)})
        events_list[i].update({'eventSec': float(time)})

    cur_date = match_df["dateutc"]

    # Creating dataframe from events list
    event_df = sp_sess.createDataFrame(events_list)
    # event_df = event_df.map(lambda x : Row(**x)).toDF()
    event_rows = [event_rows[-1]]

    # To insert into the player profile dataframe , we need to create a new df
    # And union this with the player profile
    """
    new_row = [(1134,'abc', 5, 6, 7, 0.56, 0)]
    new_df = sp_sess.createDataFrame(new_row, profile_schema)
    player_profile = player_profile.union(new_df)
    """

    # Store the previous player ratings
    prev_player_rating = player_ratings.copy()

    # Getting player IDs from the players.csv
    players_id = players.select('Id').rdd.flatMap(lambda x: x).collect()

    # Calling ret_players
    teams_dict = filter_players(ret_players(match_df), players_id)

    # Calculating metrics 1
    pass_ac = pass_accuracy(event_df)
    duel_eff = duel_effectiveness(event_df)
    free_eff = freekick_effectiveness(event_df)
    shots_eff, goals_per_player = shots_effectiveness(event_df)
    fouls_per_player = fouls_loss(event_df)
    own_per_player = own_goal(event_df)

    # Calculating metrics 2
    player_contribution = player_contribution_main(
        match_df, pass_ac, duel_eff, free_eff, shots_eff)

    # If record is invalid
    if (player_contribution == None):
        return

    # Player rating for the players
    player_ratings = player_rating(
        player_ratings, player_contribution, own_per_player, fouls_per_player)

    # Player chemistry for the players
    player_chemistry = calc_chemistry(
        player_chemistry, player_ratings, prev_player_rating, teams_dict)

    # Player profile, clustering and regression
    player_profile = get_profile(player_profile, fouls_per_player, own_per_player, goals_per_player, pass_ac, shots_eff, teams_dict)
    regr_player = linreg_predict(player_profile, player_ratings, cur_date , teams_dict, regr_player)

    # Testing with Random data for test
    # # Generating random data
    # for i in range(100):
    #     random.seed(i)
    #     tuple_dat = (random.randint(1000, 10000), (random.random()))
    #     rand_data.append(tuple_dat)

    #     # Creating the dataframe
    # rand_df = sp_sess.createDataFrame(data=rand_data, schema=cols)

    # Training the model
    make_model(regr_player, False)

    c = list(teams_dict.keys())

    team1 = teams_dict[c[0]]
    team2 = teams_dict[c[1]]

    '''
        (49876, 8066)
        (49876, 217078)
        (93, 254898)
        (93, 3324)
        (93, 212651)
        (93, 135103)
        (93, 227756)
    '''
def process_record(rdd):

    # If RDD is empty, move on
    if rdd.isEmpty():
        return

    # Using global variables
    global event_rows
    global match_count

    match_count += 1
    print('Current Match:', match_count)

    # Stopping the stream
    if (match_count > 5):
        with open("match_details.json","a") as file:
            file.write(']')
        # Saving the data to HDFS
        save_data()
        

    # Collecting match and event data
    stream_json = rdd.map(lambda x : eval(x))
    for row in stream_json.collect():
        event_rows.append(row)

        # Match data encountered
        if 'teamsData' in row.keys():
            match_data()
        else:
            continue
        
def write_to_file(file_name, content):
    """
    write_to_file : To write to JSON file

    Arguments:
        file_name {str} -- File name for JSON
        content {str} -- JSON string
    """

    with open(file_name, 'w') as file:
        file.write(content)

# Saving all Data to storage
def save_data():
    """
    save_data : Saves the data to HDFS
    """

    # Global variables
    global player_chemistry
    global player_ratings
    global player_profile
    global match_details

    # Player chem keys as strings
    player_chemistry = {str(key): value for key,
                        value in player_chemistry.items()}

    # Creating JSON strings using JSON
    player_chem_json = json.dumps(player_chemistry, indent=4)
    player_rate_json = json.dumps(player_ratings, indent=4)
    player_prof_json = json.dumps(player_profile, indent=4)

    # Saving the model
    import metrics
    metrics.curr_model.write().overwrite().save('reg_model')

    # Writing the JSON strings to files
    write_to_file("player_chem.json", player_chem_json)
    write_to_file("player_rate.json", player_rate_json)
    write_to_file("player_profile.json", player_prof_json)
    ssp_context.stop()

if __name__ == '__main__':

    # Creating the streaming spark context and spark session
    sp_context = SparkContext('local[2]', "Read_Stream")
    ssp_context = StreamingContext(sp_context, 5.000)
    sp_sess = SparkSession.builder.appName('Read_Data').getOrCreate()

    # Adding the files to Spark Context
    # sp_context.addFile("calc_stats_1.py")
    # sp_context.addFile("calc_stats_2.py")
    # sp_context.addFile("init_stats.py")
    # sp_context.addFile("model.py")
    # sp_context.addFile("chances_of_winning.py")
    sp_context.addFile('metrics.py')

    # Reading the CSV files using Spark session
    players = sp_sess.read.csv(play_path, header=True, inferSchema=True)
    teams = sp_sess.read.csv(team_path, header=True, inferSchema=True)

    # Importing the files
    from metrics import *

    """global player_chemistry
    global player_ratings
    global regr_player
    global player_profile
    global match_details
"""
    # Initializing the player chem and ratings
    player_chemistry = init_chemistry(players)
    player_ratings = init_ratings(players)
    regr_player = init_regr()

    # Adds player profile
    player_profile = init_profile()
    # player_profile.show()
    with open("match_details.json","a") as file:
            file.write('[')

    """
    Need to convert following into dataframe and then into JSON
    () player_chemistry
    () player_profile
    () player_ratings
    () Cluster 
    (*) Model(Regression)
    """

    """"player_chem_json = json.dumps(player_chemistry, indent=4)
    player_rate_json = json.dumps(player_ratings, indent=4)
    player_prof_json = json.dumps(player_profile, indent=4)"""

    # Writing the JSON strings to files
    """write_to_file('player_chem.json', player_chem_json)
    write_to_file('player_rate.json', player_rate_json)
    write_to_file('player_profile.json', player_prof_json)"""

    # Connecting to the specified host and port number
    data = ssp_context.socketTextStream('localhost', 6100)
    data.foreachRDD(process_record)
    ssp_context.start()
    ssp_context.awaitTermination()
