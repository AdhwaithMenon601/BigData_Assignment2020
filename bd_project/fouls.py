import sys
import json
fouls_per_player = {}
def fouls_loss(event_df):
    fouls_df = event_df.filter(event_df['eventId'] == 2)
    rows = fouls_df.select("playerId","tags").collect()
    if (len(rows) == 0):
        return fouls_per_player
    for i in rows:
        player_id = i.playerId
        if (not(player_id in fouls_per_player)):
            fouls_per_player[player_id] = 1
        else:
            fouls_per_player[player_id] += 1
    return fouls_per_player