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
        for j in tags:
            total_size += 1
            if (j['id'] == 1801):
                num_eff_free += 1
            if (i.subEventId == 35 and j['id'] == 101):
                num_penalty += 1
        if (total_size == 0):
            free_kick[player_id] = 0
            continue
        free_kick_eff = (num_eff_free + num_penalty) / len(i)
        free_kick[player_id] = free_kick_eff
    return free_kick