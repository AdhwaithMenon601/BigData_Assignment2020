import sys
import json
import gc
from pyspark.sql import functions as func
from pyspark.sql.types import BooleanType


# Calculating the player rating
def player_rating(player_contribution, own_per_player, fouls_per_player):
    """
    player_rating : The rating per player for a match

    Arguements:
        player_contribution {dict} -- Dictionary of separate player contributions
        own_per_player {dict} -- Dictionary of own goals per player
        fouls_per_player {dict} -- Dictionary of number of fouls per player
    
    Returns:
        dict -- player rating per player
    """
    player_rate = {}

    # Finding the performance from contribution
    for i in player_contribution:
        if (not(i in player_rate)):
            player_rate[i] = 0.5
            continue

        # Edge cases
        if (not(i in fouls_per_player)):
            fouls_per_player[i] = 0
        if (not(i in own_per_player)):
            own_per_player[i] = 0

        # Finding performance 
        player_perf = player_contribution[i] - ( (0.005 * fouls_per_player[i]) + (0.5 * own_per_player[i]) )
        player_rate[i] = (player_rate[i] + player_perf) / 2
    
    return player_rate