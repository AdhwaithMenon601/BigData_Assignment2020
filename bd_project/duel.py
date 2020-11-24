import sys
import json
players_duel={}
def duel_effectiveness(event_df):
	# Selecting all duels
	duel_rdd=event_df.filter(event_df['eventId']==1)
	row_list=duel_rdd.select("playerId","tags").collect()
	for i in row_list:
		count={701:0,702:0,703:0}
		player_id=i.playerId
		tags=i.tags
		for j in tags:
			if j['id'] in count:
				count[j['id']]+=1
		de=(count[703]+(count[701]*0.5))/(count[701]+count[702]+count[703])
		players_duel[player_id]=de
	return players_duel
		
