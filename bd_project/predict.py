import sys
import json
import itertools
import csv
import string
teams = []
with open('teams.csv') as csvfile:
    csvreader = csv.reader(csvfile) 
    for i in csvreader:
        teams.append(i)
teams = teams[1:]
players=[]
data = {}
with open('players.csv') as csvfile: 
    csvreader = csv.reader(csvfile) 
    for i in csvreader:
        x = json.dumps(i[0])
        y = json.loads(x)
        players.append((y,i[4]))
players = players[1:]

def predict(user):
    gk1 = 0
    df1 = 0
    md1 = 0
    fw1 = 0
    gk2 = 0
    df2 = 0
    md2 = 0
    fw2 = 0
    team_name_1= user['team1']['name']
    team_name_2 = user['team2']['name']
    players_name_team_1 = []
    players_name_team_2 = []
    # for i in user['team1']
    temp = list(user['team1'])
    temp = temp[1:]
    for i in temp:
        #print(user['team1'][i].encode('utf-8'))
        players_name_team_1.append(r'{}'.format(user['team1'][i]))
    temp = list(user['team2'])
    temp = temp[1:]
    for i in temp:
        players_name_team_2.append(user['team2'][i])
    for i in players_name_team_1:
        for j in players:
            if j[0]==i:
                if j[1]=='FW':
                    fw1+=1 
                elif j[1]=='GK':
                    gk1+=1
                elif j[1]=='MD':
                    md1+=1
                elif j[1]=='DF':
                    df1+=1
    #print(players_name_team_1,players_name_team_2)
    for i in players_name_team_2:
        for j in players:
            #x =  r'{}'.format(i)
            y =  r'{}'.format(j[0])
            #print(x,y)
            if x==y:
                #print(j[0])
                if j[1]=='FW':
                    fw2+=1 
                elif j[1]=='GK':
                    gk2+=1
                elif j[1]=='MD':
                    md2+=1
                elif j[1]=='DF':
                    df2+=1
    #print(players)
    #print(gk1,gk2,fw1,fw2,md1,md2,df1,df2)
    if gk1==1 and df1>=3 and md1>=2 and fw2>=1 and gk2==1 and df2>=3 and md2>=2 and fw2>=1 :
        print("hello")
    else:
        print("NOT POSSIBLE")
