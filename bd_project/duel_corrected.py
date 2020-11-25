import sys
import json
def duel_effectiveness(event_df):
	# Selecting all duels
	duel_rdd=event_df.filter(event_df['eventId']==1)
	row_list=duel_rdd.select("playerId","tags").collect()
	# key : player_id
	# Value : [701 count,702 count,703 count]
	valid_list=[701,702,703]
	players_count={}
	players_duel={}
	for i in row_list:
		#count={701:0,702:0,703:0}
		player_id=i.playerId
		tags=i.tags
		if player_id not in players_count:
			players_count[player_id]={701:0,702:0,703:0}
		for j in tags:
			if j['id'] in valid_list:
				players_count[player_id][j['id']]+=1
				#count[j['id']]+=1
	#de=(count[703]+(count[701]*0.5))/(count[701]+count[702]+count[703])
	for i in players_count:
		players_duel[i]=(players_count[i][703]+(players_count[i][701]*0.5))/(players_count[i][701]+players_count[i][702]+players_count[i][703])
	return players_duel
		
