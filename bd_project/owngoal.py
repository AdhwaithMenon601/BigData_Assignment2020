import sys
import json
own_per_player = {}
def own_goal_calc(event_df):
    rows = event_df.select("playerId","tags").collect()
    for i in rows:
        player_id = i.playerId
        own_count = 0
        for j in i.tags:
            if (j['id'] == 102):
                own_count += 1
        own_per_player[player_id] = own_count
    return own_per_player