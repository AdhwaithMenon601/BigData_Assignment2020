import json 
import csv 
from pyspark.streaming import StreamingContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col
from model import *

fin = open("inp_predict.json")
input_data = json.load(fin)

# Setting the paths of the CSV files
play_path = "hdfs://localhost:9000/players.csv"
team_path = "hdfs://localhost:9000/teams.csv"

sp_sess = SparkSession.builder.appName('user_input').getOrCreate()

def isvalid(roles):
    d = {'MD':0,'GK':0,'DF':0,'FW':0}
    for i in roles:
        d[i]+=1 
    if d['MD']>=2 and d['DF']>=3 and d['FW']>=1 and d['GK']==1:
        return True
    else:
        return False

def predict_helper(user):
    players_name_team_1 = []
    players_name_team_2 = []
    #for i in user['team1']
    temp = list(user['team1'])
    temp = temp[1:]
    cur_date = user['date']
    for i in temp:
        #print(user['team1'][i].encode('utf-8'))
        x = user['team1'][i]
        players_name_team_1.append(x)
    temp = list(user['team2'])
    temp = temp[1:]
    cur_date = user['date']
    t1 = temp[0]
    t2 = temp[0]
    for i in temp:
        x = user['team2'][i]
        players_name_team_2.append(x)
    # Reading the CSV files using Spark session
    players = sp_sess.read.csv(play_path, header=True, inferSchema=True)
    #print(players_name_team_1)
    team_1 = players.filter(players["name"].isin(players_name_team_1)== True)
    team_2 = players.filter(players["name"].isin(players_name_team_2))
    team1_id = team_1.select("Id").collect()
    team2_id = team_2.select("Id").collect()
    role_team1 = team_1.select('role').collect()
    role_team2 = team_2.select('role').collect()
    teams_dict = {t1:[], t2:[]}
    team1_roles = []
    team2_roles = []
    for i in team1_id:
        teams_dict[t1].append(i.Id)
    for i in team2_id:
        teams_dict[t2].append(i.Id)
    for i in role_team1:
        team1_roles.append(i.role)
    for i in role_team2:
        team2_roles.append(i.role)
    
    for i in teams_dict[t1]:
        cur = find_rating(i, cur_date)
        if (cur < 0.2):
            print("{} has retired".format(i))
            print("Match is not valid")
    
    for i in teams_dict[t2]:
        cur = find_rating(i, cur_date)
        if (cur < 0.2):
            print("{} has retired".format(i))
            print("Match is not valid")
    
    
    if isvalid(team1_roles) and isvalid(team2_roles):
        print("We will be calling the functions here")
    else:
        print("Match is not valid")
    #req_predict = process_stream(None)
    
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
    print(name,birthArea,foot,role,height,weight)
    #player_output = process_stream(None)   

def match_data_helper(user):
    match_date = user['date']
    match_details = user['label']
    temp = match_details.split(",")
    temp1 = temp[0]
    team1,team2 = temp1.split("-")
    # team1_goal,team2_goal = temp2.split("-")
    teams = sp_sess.read.csv(team_path, header=True, inferSchema=True)
    teams.show()
    # print(team1)
    team_1 = teams.filter(teams["name"].isin(team1)== True)
    team_1.show()
    team_id_1 = team_1.select('Id').collect()[0].Id
    #print(team1,team2,team1_goal,team2_goal)
    team_2 = teams.filter(teams["name"].isin(team2)== True)
    team_id_2 = team_2.select('Id').collect()[0].Id
    print(match_date,team_id_1,team_id_2)
    # process_stream(team1, team2, date)

if __name__ == "__main__":
    
    if input_data["req_type"] == 1:
        # calling predict function:
        predict_helper(input_data)

    elif input_data["req_type"] == 2:
        # calling profile function
        player_profile_helper(input_data)
    
    else:
        # calling match info function 
        match_data_helper(input_data)
            