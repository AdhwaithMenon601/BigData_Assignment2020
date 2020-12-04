import sys
import json
import itertools
import datetime
import random

# Base Pyspark SQL functions
from pyspark.sql.functions import array, lit, datediff, col
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.types import BooleanType

# Transformers for data transformation
from pyspark.ml.linalg import Vectors 
from pyspark.ml.feature import VectorAssembler

# ML Modules for clustering and regression
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Global model variables
curr_model = None



############################################
#   FIRST LIST OF METRICS COMPUTED
############################################
def pass_accuracy(event_df):
    """
    pass_accuracy : Calculates Pass Accuracy per player
    Arguments:
        event_df {DF} -- Events Dataframe
    Returns:
        dict -- Accuracy per player
    """

    # Dicts for the Pass types
    key_pass = {'id': 302}
    acc_pass = {'id': 1801}
    inc_pass = {'id': 1802}

    # Selecting all pass events
    pass_df = event_df.filter(event_df['eventId'] == 8)
    player_stats = {}
    player_acc={}

    for record in pass_df.collect():

        if record['playerId'] not in player_stats.keys():
            player_stats.update({record['playerId']: [0, 0, 0, 0]})

        old_val = player_stats[record['playerId']]

        # Checking for accurate key pass
        if key_pass in record['tags'] and acc_pass in record['tags']:
            old_val[0] += 1

        # Checking for accurate pass
        elif acc_pass in record['tags'] and key_pass not in record['tags']:
            old_val[1] += 1

        # Checking for inaccurate key pass
        elif key_pass in record['tags'] and acc_pass not in record['tags']:
            old_val[2] += 1

        # Else normal inaccurate pass
        elif key_pass not in record['tags'] and acc_pass not in record['tags']:
            old_val[3] += 1

        player_stats.update({record['playerId']: old_val})

    # Calculating accuracy for each player
    for player in player_stats:
        stats = player_stats[player]
        acc = (stats[1] + (stats[0] * 2)) / \
            ((stats[1] + stats[3]) + ((stats[0] + stats[2]) * 2))
        player_acc.update({player: acc})

    return player_stats,player_acc


def duel_effectiveness(event_df):
    """
    duel_effectiveness : Calculates duel effectiveness per player

    Arguments:
        event_df {DF} -- Pyspark DF for events

    Returns:
        dict -- effectiveness per player
    """

    # Selecting all duels
    duel_rdd = event_df.filter(event_df['eventId'] == 1)
    row_list = duel_rdd.select("playerId", "tags").collect()

    # List of duel events
    valid_list = [701, 702, 703]
    players_duel = {}

    # Iterating over DF rows
    for i in row_list:

        player_id = i.playerId
        tags = i.tags

        # If key not found, initialize
        if player_id not in players_duel:
            players_duel[player_id] = {701: 0, 702: 0, 703: 0}

        # Indexing inner dict
        for j in tags:
            if j['id'] in valid_list:
                players_duel[player_id][j['id']] += 1

    # Calculating duel effectiveness for each player
    for i in players_duel:
        stats = players_duel[i]
        players_duel[i] = (stats[703] + (stats[701] * 0.5)) / \
            (stats[701] + stats[702] + stats[703])

    return players_duel


def freekick_effectiveness(df):
    """
    freekick_effectiveness : Calculates freekick effectiveness per player

    Arguments:
        df {DF} -- Pyspark DF for events

    Returns:
        dict -- Effectiveness per player
    """
    free_kick = {}

    # Selecting required free kicks
    free_df = df.filter(df['eventid'] == 3)
    row = free_df.select("playerId", "tags", "subEventId").collect()

    # Above has all the required data that is needed
    for i in row:

        player_id = i.playerId
        tags = i.tags

        # If player not found, initialize
        if (not(player_id in free_kick)):
            free_kick[player_id] = [0, 0, 0, 0]

        for j in tags:
            free_kick[player_id][3] += 1

            # Accurate free kick
            if (j['id'] == 1801):
                free_kick[player_id][1] += 1

            # Penalty and a goal
            if (i.subEventId == 35 and j['id'] == 101):
                free_kick[player_id][2] += 1

        # If ever this is encountered
        if (free_kick[player_id][3] == 0):
            continue

        # Calculating the effectiveness
        free_kick[player_id][0] = (free_kick[player_id][1] + free_kick[player_id][2]) / \
            free_kick[player_id][3]

    return free_kick

def shots_effectiveness(event_df):
    """
    shots_effectiveness : Shots effectiveness for each player

    Arguments:
        event_df {DF} -- Pyspark DF for events

    Returns:
        dict -- Effectiveness per player
    """
    player_shots = {}
    player_goals={}
    player_shots_effec={}

    # Event dicts
    on_target = {'id' : 1801}
    non_target = {'id' : 1802}
    goal = {'id' : 101}

    # Selecting all shots from the DF
    shots = event_df.filter(event_df['eventId'] == 10)
    for record in shots.collect():

        if record['playerId'] not in player_shots.keys():
            player_shots.update({record['playerId']: [0, 0, 0]})

        old_val = player_shots[record['playerId']]

        # Shot on target and a goal
        if on_target in record['tags'] and goal in record['tags']:
            old_val[0] += 1

        # Shot on target but not a goal
        elif on_target in record['tags'] and goal not in record['tags']:
            old_val[1] += 1

        # Shot not on target
        elif non_target not in record['tags']:
            old_val[2] += 1

        player_shots.update({record['playerId']: old_val})
        player_goals.update({record['playerId']: old_val[0]})

    # Shots effectiveness for each player
    for player in player_shots:
        stats = player_shots[player]
        if (stats[0] + stats[1] + stats[2] == 0):
            eff = 0
        else:
            eff = (stats[0] + (stats[1] * 0.5)) / (stats[0] + stats[1] + stats[2])
        player_shots_effec.update({player: eff})

    return player_shots,player_goals,player_shots_effec


def fouls_loss(event_df):
    """
    fouls_loss : No of fouls per player

    Arguments:
        event_df {DF} -- Pyspark DF for events

    Returns:
        dict -- Fouls / player
    """
    fouls_per_player = {}

    # Filterting the foul events
    fouls_df = event_df.filter(event_df['eventId'] == 2)
    rows = fouls_df.select("playerId", "tags").collect()

    # If RDD is empty
    if (len(rows) == 0):
        return fouls_per_player

    # Iterating over rows
    for i in rows:
        player_id = i.playerId

        # If key doesnt exist, initialize
        if (player_id not in fouls_per_player):
            fouls_per_player.update({player_id:1})

        # If key exists, update
        else:
            tmp = fouls_per_player[player_id] + 1
            fouls_per_player.update({player_id:tmp})

    return fouls_per_player


def own_goal(event_df):
    """
    own_goal : Own goals per player

    Arguments:
        event_df {DF} -- Pyspark DF for events

    Returns:
        dict -- Own goals / player
    """
    own_per_player = {}

    # Selecting player ID and tags
    rows = event_df.select("playerId", "tags").collect()
    for i in rows:
        player_id = i.playerId
        own_count = 0

        # If key doesnt exist, initialize
        if (player_id not in own_per_player):
            own_per_player.update({player_id:0})

        # Checking for 102 : Own goal
        for j in i.tags:
            if (j['id'] == 102):
                tmp = own_per_player[player_id] + 1
                own_per_player.update({player_id:tmp})

    return own_per_player

############################################
#   SECOND LIST OF METRICS COMPUTED
############################################

# returns player contribution for players fo each team


def player_contribution_dict(value, dtime, player_contribution, players_pass, players_duel, players_kick, players_shot):
    """
    duration=match_df.collect()[0].duration
    if duration=="Regular":
            dtime=90
    else:
            dtime=90+30
    team_data=match_df.select("teamsData").collect()[0].asDict().values()
    """
    # print(team_data)
    # print(value)
    value = value["formation"]
    subs = value["substitutions"]
    line_up = value["lineup"]

    if not (subs != 'null' and line_up != 'null' and player_contribution != None):
        return None

    subs_out = {}
    subs_in = {}
    for i in subs:
        subs_out[i["playerOut"]] = i["minute"]
        subs_in[i["playerIn"]] = i["minute"]
    players = {}
    for i in line_up:
        pid = i["playerId"]
        if pid not in players:
            if pid not in subs_out:
                players[pid] = dtime
            else:
                players[pid] = subs_out[pid]
    for i in subs_in:
        if i not in players:
            players[i] = dtime-subs_in[i]
    # players_contribution={1:1,2:2,3:3}
    for i in players:
        cont = 0
        if i in players_pass:
            cont += players_pass[i]
        if i in players_duel:
            cont += players_duel[i]
        if i in players_kick:
            cont += players_kick[i][3]
        if i in players_shot:
            cont += players_shot[i]
        cont = cont/4
        if players[i] == dtime:
            cont *= 1.05
        else:
            cont *= (players[i]/90)
        player_contribution[i] = cont
    return player_contribution

# returns player contribution for all players


def player_contribution_main(match_df, players_pass, players_duel, players_kick, players_shot):
    duration = match_df["duration"]
    if duration == "Regular":
        dtime = 90
    else:
        dtime = 90+30
    team_data = match_df["teamsData"].values()
    player_contribution = {}
    for i in team_data:
        player_contribution_dict(i, dtime, player_contribution,
                                 players_pass, players_duel, players_kick, players_shot)
    return player_contribution


# Calculating the player rating
def player_rating(player_rating, player_contribution, player_profile):
    """
    player_rating : The rating per player for a match

    Arguements:
        player_contribution {dict} -- Dictionary of separate player contributions
        own_per_player {dict} -- Dictionary of own goals per player
        fouls_per_player {dict} -- Dictionary of number of fouls per player
        player_rate {dict} -- Dictionary for each player rating

    Returns:
        void
    """

    temp_var = 0
    # Finding the performance from contribution
    for i in player_contribution:
        if (i not in player_rating):
            player_rating.update({i:0.5})
            continue

        # Edge cases
        if (i not in player_profile):
            player_profile.update({i:[0, "", 0, 0, 0, 0, 0]})

        # Finding performance
        player_perf = player_contribution[i] - \
            (((0.005 * player_profile[i][0]) * player_contribution[i]) + ((0.05 * player_profile[i][3]) * player_contribution[i]))
        temp_var = ((player_rating[i] + player_perf) / 2)
        player_rating.update({i : temp_var})
    
    return player_rating

# For calculating the chemistry after each match


def calc_chemistry(player_chemistry, player_rating, prev_player_rating, team_player_dict):
    """
    calc_chemistry : The chemistry per player pair for a match

    Arguements:
        player_chemistry {dict} -- Dictionary of separate player chemistries
        player_rating {dict} -- Rating dictionary of player
        team_player_dict {dict} -- Dictionary of team id and players playing

    Returns:
        void

        (49876, 350976)
        (49876, 8066)
        (49876, 217078)
        (93, 254898)
        (93, 3324)
        (93, 212651)
        (93, 135103)
        (93, 227756)


    """
    unique_teams = list(team_player_dict.keys())

    # Compute chemistry for players of individual team
    # First for team 1
    var = 0
    for player_pair in itertools.combinations(team_player_dict[unique_teams[0]], 2):
        # Since these are for the same team , chemistry is updated as such
        p1, p2 = player_pair
        temp_pair = (p2, p1)
        v = player_pair in player_chemistry
        s = temp_pair in player_chemistry
        chem_change = 0

        if (p1 not in player_rating):
            player_rating.update({p1:0.5})
        if (p2 not in player_rating):
            player_rating.update({p2:0.5})

        p1_change = player_rating[p1] - prev_player_rating[p1]
        p2_change = player_rating[p2] - prev_player_rating[p2]

        # If ratings of both players either increases or decreases differently then we increase
        if ((p1_change <= 0 and p2_change <= 0) or (p1_change >= 0 and p2_change >= 0)):
            chem_change = abs((abs(p1_change) - abs(p2_change)) / 2)
        # If ratings of both players increases or decreases same way , then we reduce
        elif ((p1_change <= 0 and p2_change >= 0) or (p1_change >= 0 and p2_change <= 0)):
            temp_change = abs((abs(p1_change) - abs(p2_change)) / 2)
            chem_change = temp_change * -1

        # Updating the player chemistry required
        if (player_pair not in player_chemistry):
            if (temp_pair not in player_chemistry):
                continue
            else:
                var = player_chemistry[temp_pair] + chem_change
                var = max(0,var)
                var = min(1,var)
                player_chemistry.update({temp_pair: var})
        else:
            var = player_chemistry[player_pair] + chem_change
            var = max(0,var)
            var = min(1,var)
            player_chemistry.update({player_pair: var})

    # Next for team 2
    for player_pair in itertools.combinations(team_player_dict[unique_teams[1]], 2):
        # Since these are for the same team , chemistry is updated as such
        p1, p2 = player_pair
        temp_pair = (p2, p1)
        chem_change = 0

        if (p1 not in player_rating):
            player_rating.update({p1:0.5})
        if (p2 not in player_rating):
            player_rating.update({p2:0.5})

        p1_change = player_rating[p1] - prev_player_rating[p1]
        p2_change = player_rating[p2] - prev_player_rating[p2]

        # If ratings of both players either increases or decreases differently then we increase
        if ((p1_change <= 0 and p2_change <= 0) or (p1_change >= 0 and p2_change >= 0)):
            chem_change = abs((abs(p1_change) - abs(p2_change)) / 2)
        # If ratings of both players increases or decreases same way , then we reduce
        elif ((p1_change <= 0 and p2_change >= 0) or (p1_change >= 0 and p2_change <= 0)):
            temp_change = abs((abs(p1_change) - abs(p2_change)) / 2)
            chem_change = temp_change * -1

        # Updating the player chemistry required
        if (player_pair not in player_chemistry):
            if (temp_pair not in player_chemistry):
                continue
            else:
                var = player_chemistry[temp_pair] + chem_change
                var = max(0,var)
                var = min(1,var)
                player_chemistry.update({temp_pair: var})
        else:
            var = player_chemistry[player_pair] + chem_change
            var = max(0,var)
            var = min(1,var)
            player_chemistry.update({player_pair: var})
    

    # Next for combined of teams 1 and teams 2
    combined_team_list = [(i, j) for i in team_player_dict[unique_teams[0]]
                          for j in team_player_dict[unique_teams[1]]]
    for player_pair in combined_team_list:
        p1, p2 = player_pair
        temp_pair = (p2, p1)
        chem_change = 0

        if (p1 not in player_rating):
            player_rating.update({p1:0.5})
        if (p2 not in player_rating):
            player_rating.update({p2:0.5})

        p1_change = player_rating[p1] - prev_player_rating[p1]
        p2_change = player_rating[p2] - prev_player_rating[p2]

        # If ratings of both teams either increases or decreases differently then we increase
        if ((p1_change <= 0 and p2_change >= 0) or (p1_change >= 0 and p2_change <=0)):
            chem_change = abs((abs(p1_change) - abs(p2_change)) / 2)
        # If ratings of both teams increases or decreases same way , then we reduce
        elif ((p1_change >= 0 and p2_change >= 0) or (p1_change <= 0 and p2_change <= 0)):
            temp_change = abs((abs(p1_change) - abs(p2_change)) / 2)
            chem_change = temp_change * -1

        # Updating the player chemistry required
        if (player_pair not in player_chemistry):
            if (temp_pair not in player_chemistry):
                continue
            else:
                var = player_chemistry[temp_pair] + chem_change
                var = max(0,var)
                var = min(1,var)
                player_chemistry.update({temp_pair: var})
        else:
            var = player_chemistry[player_pair] + chem_change
            var = max(0,var)
            var = min(1,var)
            player_chemistry.update({player_pair: var})
        
    return player_chemistry


def ret_players(match_df):

    team_id = list(match_df['teamsData'].keys())
    teams_dict = {}
    
    # Bench information for the team
    bench_1 = match_df['teamsData'][team_id[0]]['formation']['bench']
    bench_2 = match_df['teamsData'][team_id[1]]['formation']['bench']
    bench_id_1 = [id['playerId'] for id in bench_1]
    bench_id_2 = [id['playerId'] for id in bench_2]

    # Lineup information for the teams
    lineup_1 = match_df['teamsData'][team_id[0]]['formation']['lineup']
    lineup_2 = match_df['teamsData'][team_id[1]]['formation']['lineup']
    lineup_id_1 = [player['playerId'] for player in lineup_1]
    lineup_id_2 = [player['playerId'] for player in lineup_2]

    # Team players for each team
    team_1 = bench_id_1 + lineup_id_1
    team_2 = bench_id_2 + lineup_id_2
    teams_dict[team_id[0]] = team_1
    teams_dict[team_id[1]] = team_2
    
    return teams_dict


################################################################
#   PREDICTING PLAYER RATING USING LINEAR REGRESSION
################################################################

def find_rating(player_id, cur_date):
    sp_sess = SparkSession.builder.appName('Regr_Data').getOrCreate()
    play_path = "hdfs://localhost:9000/players.csv"
    players = sp_sess.read.csv(play_path, header=True, inferSchema=True)
    assembler = VectorAssembler(inputCols = ['new_diff'],outputCol = 'features')
    name_df = players.filter(players['Id'] == int(player_id))
    
    player_date = name_df.select("birthDate").collect()[0].birthDate

    new_date1 = player_date.split('-')
    new_date2 = cur_date.split('-')


    d1 = datetime.date(int(new_date1[0]), int(new_date1[1]), int(new_date1[2]))
    d2 = datetime.date(int(new_date2[0]), int(new_date2[1]), int(new_date2[2]))


    diff = abs(d2 - d1).days
    my_rating = 1.000
    my_schema = StructType([
        StructField('diff', IntegerType(), True),
        StructField('rating', FloatType(), True)
    ])
    my_dict = {'diff': diff, 'rating': my_rating}

    new_df = sp_sess.createDataFrame([my_dict], my_schema)

    new_df = new_df.withColumn('new_diff', new_df['diff'] / 1000)
    new_df = new_df.withColumn('new_rating', new_df['rating'] * 10)

    test = assembler.transform(new_df)
    final_model = LinearRegressionModel.load('reg_model')

    res = final_model.evaluate(test)

    req = res.predictions.select("prediction").rdd.flatMap(lambda x : x).collect()

    final_res = req[0] / 10

    if (final_res > 1):
        final_res /= 2
    if (final_res > 0.9):
        final_res /= 2

    return abs(final_res)

# Code for linear regression model
def make_model(player_regr, initial_run):

    global curr_model
    assembler_1 = VectorAssembler(inputCols = ['diff'],outputCol = 'features')
    lr = None
    train_df = None
    test_df = None

    # For non-initial run of model
    if initial_run == False:

        # Setting Old Model's weights
        player_regr = player_regr.withColumn('weights', lit(abs(curr_model.coefficients[0])))
        check_data = player_regr.select("diff", "rating", 'weights')

        check_data = check_data.withColumn('new_diff', check_data['diff'] / 1000)
        check_data = check_data.withColumn('new_rating', check_data['rating'] * 10)

        assembler_2 = VectorAssembler(inputCols = ['new_diff'],outputCol = 'features')

        # Splitting the data
        train,test = check_data.randomSplit([0.7,0.3])

        # Transforming the data
        train_df = assembler_2.transform(train)
        test_df = assembler_2.transform(test)

        # LR Object
        lr = LinearRegression(featuresCol='features', labelCol='new_rating', weightCol='weights')

    else:
        check_data = player_regr.select("diff", "rating")

        # Splitting the data
        train,test = check_data.randomSplit([0.7,0.3])

        # Transforming the data
        train_df = assembler_1.transform(train)
        test_df = assembler_1.transform(test)

        # LR Object
        lr = LinearRegression(featuresCol='features', labelCol='rating')

    curr_model = lr.fit(train_df)


########################################################
#   CLUSTERING AND PREDICTING CHANCES OF WINNING
########################################################


def prepare_dataframe(player_profile):
    sp_sess = SparkSession.builder.appName('Read_Data').getOrCreate()
    player_new_profile = []
    tempd = {}
    #print(player_profile)
    r_schema = StructType([StructField('player_id', StringType(), True), StructField('fouls', IntegerType(), True), StructField('name', StringType(), True), StructField('goals', IntegerType(
    ), True), StructField('owngoals', IntegerType(), True), StructField('pass_acc', FloatType(), True), StructField('shots', FloatType(), True), StructField('matches', IntegerType(), True)])
    for i in player_profile:
        tempd = {}
        tempd["player_id"] = i
        temp = player_profile[i]
        tempd["fouls"] = temp[0]
        tempd["name"] = temp[1]
        tempd["goals"] = temp[2]
        tempd["owngoals"] = temp[3]
        tempd["pass_acc"] = float(temp[4])
        tempd["shots"] = float(temp[5])
        tempd["matches"] = temp[6]
        player_new_profile.append(tempd)
        print(tempd)
    player_df = sp_sess.createDataFrame(player_new_profile, r_schema)
    print("THE COUNT IS ", player_df.count())
    return player_df


def clustering(player_profile):
    vecAssembler = VectorAssembler(
        inputCols=["fouls", "goals", "owngoals", "pass_acc", "shots", "matches"], outputCol="features")
    print(player_profile.printSchema())
    player_profile = player_profile.drop("name")
    new_df = vecAssembler.transform(player_profile)
    kmeans = KMeans(k=5)
    model = kmeans.fit(new_df.select("features"))
    # Make predictions
    predictions = model.transform(new_df)
    predictions.show()
    # Evaluate clustering by computing Silhouette score
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    print("Silhouette with squared euclidean distance = " + str(silhouette))
    # Shows the result.
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)
    return predictions, centers


def predict(player_chem, player_profile, player_ratings, team1, team2, cur_date):
    player_coeff1 = {}
    lessthan1 = []
    for i in team1:
        player = i
        team1.remove(i)
        total1 = 0
        if  str(i) in player_profile and player_profile[str(i)][6] >= 5:
            for j in team1:
                tup = '(' + str(i) + ',' + ' ' + str(j) + ')'
                if tup in player_chem:
                    total1 += player_chem[tup]
                else:
                    tup1 = '(' + str(j) + ',' + ' ' + str(i) + ')'
                    total1 += player_chem[tup1]
            total1 = total1/len(team1)-1
            player_coeff1[i] = total1
        else:
            lessthan1.append(i)
    tot1 = 0
    for i in player_coeff1:
        tot1 += (player_coeff1[i]*find_rating(i, cur_date))
    player_coeff2 = {}
    lessthan2 = []
    for i in team2:
        player = i
        team2.remove(i)
        total2 = 0
        if str(i) in player_profile and player_profile[str(i)][6] >= 5:
            for j in team2:
                tup = '(' + str(i) + ',' + ' ' + str(j) + ')'
                if tup in player_chem:
                    total2 += player_chem[tup]
                else:
                    tup1 = '(' + str(j) + ',' + ' ' + str(i) + ')'
                    total2 += player_chem[tup1]
            total2 = total2/len(team2)-1
            player_coeff2[i] = total2
        else:
            lessthan2.append(i)
    tot2 = 0
    for i in player_coeff1:
        tot2 += (player_coeff1[i]*find_rating(i, cur_date))
    player_df = prepare_dataframe(player_profile)
    predictions, centers = clustering(player_df)
    #print("LESSTHAN1 ",lessthan1)
    
    for i in lessthan1:
        tempdf = predictions.filter(predictions["player_id"] == i)
        cluster_id = predictions.filter(
            predictions["prediction"] == tempdf["prediction"])
        temp_ids_df = cluster_id.select("player_id").collect()
        temp_ids1 = []
        for j in temp_ids_df:
            j_dict = j.asDict()
            temp_ids1.append(j_dict["player_id"])
        tot = 0
        for k in temp_ids1:
            if str(k) in player_ratings and k in player_coeff1:
                tot += (player_ratings[str(k)]*player_coeff1[k])
        tot = tot/len(temp_ids_df)
        tot1 += tot

    for i in lessthan2:
        tempdf = predictions.filter(predictions["player_id"] == i)
        cluster_id = predictions.filter(
            predictions["prediction"] == tempdf["prediction"])
        temp_ids_df = cluster_id.select("player_id").collect()
        temp_ids2 = []
        for j in temp_ids_df:
            j_dict = j.asDict()
            temp_ids2.append(j_dict["player_id"])
        tot = 0
        for k in temp_ids2:
            if str(k) in player_ratings and k in player_coeff2:
                tot += (player_ratings[str(k)]*player_coeff2[k])
                print("HELLO")
                print(tot)

        tot = tot/len(temp_ids_df)
        tot2 += tot
    strength_a = float(tot1/11)
    strength_b = float(tot2/11)
    chance_a = (0.7+strength_a-((strength_a+strength_b)/2))*100
    chance_b = 100-chance_a

    return chance_a, chance_b


########################################################
#   INITIALIZING THE PLAYER PROFILES AND STATS
#######################################################

import sys
import json
import itertools
import gc
from pyspark.sql import functions as func
from pyspark.sql.types import BooleanType

# Initialising the chemistry
def init_chemistry(players):
    """
    init_chemistry : Initialises the chemistry for all players

    Arguements:
        players {df} -- Dataframe of all the players
    
    Returns:
        dict -- player chemistry per player pair
    """
    # Converts the player id from the player df into a list of IDs
    player_list = [i['Id'] for i in players.rdd.collect()]
    player_chemistry = {}
    # The above is useful when we need to prepare the initial chemistry
    for chem_pair in itertools.combinations(player_list, 2):
        player_chemistry[chem_pair] = 0.5
    
    return player_chemistry

# Initialising the ratings
def init_ratings(players):
    """
    init_ratings : Initialises the ratings for all players

    Arguements:
        players {df} -- Dataframe of all the players
    
    Returns:
        dict -- player ratings per player pair
    """
    # Converts the player id from the player df into a list of IDs
    player_list = [i['Id'] for i in players.rdd.collect()]
    player_ratings = {}
    # The above is useful when we need to prepare the initial chemistry
    for player in player_list:
        player_ratings[player] = 0.5
    
    return player_ratings

