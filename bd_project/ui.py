import json 
import csv 
from pyspark.streaming import StreamingContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col
from model import *
from chances_of_winning import *
import sys

# Setting the paths of the CSV files
play_path = "hdfs://localhost:9000/ip/players.csv"
team_path = "hdfs://localhost:9000/ip/teams.csv"
#hdfspath_for_player_profile = "hdfs://localhost:9000/player_prof.json"
#hdfspath_for_match_info = "hdfs://localhost:9000/match_details.json"
def isvalid(roles):
    d = {'MD':0,'GK':0,'DF':0,'FW':0}
    for i in roles:
        d[i]+=1 
    if d['MD']>=2 and d['DF']>=3 and d['FW']>=1 and d['GK']==1:
        return True
    else:
        return False
# Prediction helpers
def predict_helper(user):

    players_name_team_1 = []
    players_name_team_2 = []
    # for i in user['team1']
    temp = list(user['team1'])
    temp = temp[1:]
    cur_date = user['date']
    for i in temp:
        # print(user['team1'][i].encode('utf-8')
        players_name_team_1.append(user['team1'][i])
    temp = list(user['team2'])
    temp = temp[1:]
    cur_date = user['date']
    t1 = temp[0]
    t2 = temp[0]
    for i in temp:
        players_name_team_2.append(user['team2'][i])
    # Reading the CSV files using Spark session
    players = sp_sess.read.csv(play_path, header=True, inferSchema=True)
    # print(players_name_team_1)
    team_1 = players.filter(players["name"].isin(players_name_team_1) == True)
    team_1.show()
    team_2 = players.filter(players["name"].isin(players_name_team_2))
    team1_id = team_1.select("Id").collect()
    team2_id = team_2.select("Id").collect()
    role_team1 = team_1.select('role').collect()
    role_team2 = team_2.select('role').collect()
    teams_dict = {t1: [], t2: []}
    team1_roles = []
    team2_roles = []
    for i in team1_id:
        teams_dict[t1].append(i.Id)
        print(i.Id)
    for i in team2_id:
        teams_dict[t2].append(i.Id)
        print(i.Id)
    for i in role_team1:
        team1_roles.append(i.role)
    for i in role_team2:
        team2_roles.append(i.role)

    flag = 0
    team1_retired = []
    team2_retired = []
    for i in teams_dict[t1]:
        cur = find_rating(i, cur_date)
        if (cur < 0.2):
            #print("{} has retired".format(i))
            #print("Match is not valid")
            team1_retired.append(i)
            flag = 1

    for i in teams_dict[t2]:
        cur = find_rating(i, cur_date)
        if (cur < 0.2):
            #print("{} has retired".format(i))
            # print("Match is not valid")
            team2_retired.append(i)
            flag = 1
    # def predict(player_chem, player_profile, player_ratings, team1, team2):
    if isvalid(team1_roles) and isvalid(team2_roles) and flag == 0:
        j1 = open("player_profile.json", "r")
        j1_data = j1.read()
        player_profile = eval(j1_data)
        j2 = open("player_chem.json", "r")
        j2_data = j2.read()
        player_chem = eval(j2_data)
        j3 = open("player_rate.json", "r")
        j3_data = j3.read()
        player_rate = eval(j3_data)
        temp = predict(player_chem, player_profile, player_rate, teams_dict[t1], teams_dict[t2])
        team_1_prediction,team_2_prediction = temp[0],temp[1]
        dictionary = {
            "team1":
            {
                "name":user['team1']['name'],
                "winning chance":team_1_prediction
            },
            "team2":
            {
                "name":user['team2']['name'],
                "winning chance":team_2_prediction
            }
        }
        json_object = json.dumps(dictionary, indent=4)
        # Writing to sample.json
        with open("output_req_1.json", "w") as outfile:
            outfile.write(json_object)
    elif flag==1:
        dictionary = {
            "isInvalid": 'True',
            "team1_retired_players":team1_retired,
            "team2_retired_players":team2_retired,
            }
        json_object = json.dumps(dictionary, indent=4)
        # Writing to sample.json
        with open("output_req_1.json", "w") as outfile:
            outfile.write(json_object)

    else:
        dictionary = {"isInvalid": 'True'}
        json_object = json.dumps(dictionary, indent=4)
        # Writing to sample.json
        with open("output_req_1.json", "w") as outfile:
            outfile.write(json_object)

def player_profile_helper(user):
    
    players = sp_sess.read.csv(play_path, header=True, inferSchema=True)
    player_name = user['name']
    player_info = players.filter(players["name"].isin(player_name)== True)
    name = player_info.select('name').collect()[0].name
    birthArea = player_info.select('birthArea').collect()[0].birthArea
    foot = player_info.select('foot').collect()[0].foot
    role = player_info.select('role').collect()[0].role
    height = player_info.select('height').collect()[0].height
    weight = player_info.select('weight').collect()[0].weight
    player_id = player_info.select('Id').collect()[0].Id
    with open("player_profile.json", 'r') as file:
        content = file.read()
        players_dict = eval(content)
        for i in players_dict:
            if str(i)==str(player_id):
                fouls = players_dict[i][0]
                goals = players_dict[i][2]
                own_goals = players_dict[i][3]
                pass_accuracy = players_dict[i][4]
                shots_acc = players_dict[i][4]
                dictionary ={ 
                "name" : name, 
                "birthArea":birthArea,
                "foot":foot,
                "role":role,
                "height":height,
                "weight":weight,
                "fouls":fouls,
                "goals":goals,
                "own_goals":own_goals,
                "percent_pass_accuracy": pass_accuracy,
                "percent_shots_on_target": shots_acc
                } 
                json_object = json.dumps(dictionary) 
                
                # Writing to sample.json 
                with open("output_req_2.json", "w") as outfile: 
                    outfile.write(json_object) 
                break

def match_data_helper(user):
    match_date = user['date']
    match_details = user['label']
    temp = match_details.split(",")
    temp1 = temp[0]
    team1,team2 = temp1.split("-")
    team1 = team1.strip()
    team2 = team2.strip()
    # team1_goal,team2_goal = temp2.split("-")
    teams = sp_sess.read.csv(team_path, header=True, inferSchema=True)
    # teams.show()
    # print(team1)
    team_1 = teams.filter(teams["name"].isin(team1)== True)
    # team_1.show()
    team_id_1 = team_1.select('Id').collect()[0].Id
    #print(team1,team2,team1_goal,team2_goal)
    team_2 = teams.filter(teams["name"].isin(team2)== True)
    team_id_2 = team_2.select('Id').collect()[0].Id
    #print(match_date,team_id_1,team_id_2)
    # process_stream(team1, team2, date)
    team_1 = team_1.select('name').collect()[0].name
    team_2 = team_2.select('name').collect()[0].name
    with open("match_details.json", 'r') as file:
        content = file.read()
        match_info = eval(content)
        for i in match_info:
            date_time = i['date']
            teams = i['teams_playing']
            temp = date_time.split(" ")
            date = temp[0]
            new_teams = sp_sess.read.csv(team_path, header=True, inferSchema=True)
            new_df = new_teams.filter(new_teams["Id"] == i['winner'])
            winner_name = new_df.select("name").collect()[0].name
            team = teams.split("-")
            team_a,team_b = team[0],team[1]
            team_a = team_a.strip()
            team_b = team_b.strip()
            if ((date == match_date) and ((team_1==team_a and team_2==team_b) or (team_1==team_b and team_2 == team_a))):
                dictionary = {
                    "date":match_date,
                    "duration":i['duration'],
                    "winner":winner_name,
                    "venue":i['venue'],
                    "gameweek":i['gameweek'],
                    "goals":i['goals'],
                    "own_goals":i['own_goals'],
                    "yellow_cards":i['yellow_cards'],
                    "red_cards":i['red_cards']
                }
                json_object = json.dumps(dictionary, indent=4) 
                # Writing to sample.json 
                with open("output_req_2.json", "w") as outfile:
                    print("Writing....to JSON") 
                    outfile.write(json_object) 
                break    



if __name__ == "__main__":
    sp_context = SparkContext('local[2]', "UI")
    sp_sess = SparkSession.builder.appName('user_input').getOrCreate()
    sp_context.addFile("model.py")
    input_file = sys.argv[1]
    with open(input_file, 'r') as file:
        content = file.read()
        input_data = eval(content)
        if input_data["req_type"] == 1:
            # calling predict function:
            """
            output = predict(input_)
            """
            predict_helper(input_data)

        elif input_data["req_type"] == 2:
            # calling profile function
            player_profile_helper(input_data)
        
        elif input_data["req_type"] == 3:
            # calling match info function 
            match_data_helper(input_data)
            
