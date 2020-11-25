import sys
import json
# Function for free kick
free_kick = {}
def free_kick_calc(df):
    # Selecting required free kicks
    free_df = df.filter(df['eventid'] == 3)
    row = free_df.select("playerId","tags","subEventId").collect()
    # Above has all the required data that is needed
    for i in row:
        player_id = i.playerId
        num_eff_free = 0
        num_penalty = 0
        total_size = 0
        tags = i.tags
        if (not(player_id in free_kick)):
            free_kick[player_id] = [0, 0, 0, 0]
        for j in tags:
            free_kick[player_id][3] += 1
            if (j['id'] == 1801):
                free_kick[player_id][1] += 1
            if (i.subEventId == 35 and j['id'] == 101):
                free_kick[player_id][2] += 1
        # If ever this is encountered
        if (free_kick[player_id][3] == 0):
            continue
        free_kick[player_id][0] = (free_kick[player_id][1] + free_kick[player_id][2]) / free_kick[player_id][3]
    return free_kick