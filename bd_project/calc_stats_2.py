import sys
import json
import itertools
import gc
from pyspark.sql import functions as func
from pyspark.sql.types import BooleanType

# Calculating the player rating
def player_rating(player_rating, player_contribution, own_per_player, fouls_per_player):
    """
    player_rating : The rating per player for a match

    Arguements:
        player_contribution {dict} -- Dictionary of separate player contributions
        own_per_player {dict} -- Dictionary of own goals per player
        fouls_per_player {dict} -- Dictionary of number of fouls per player
        player_rate {dict} -- Dictionary for each player rating
    
    Returns:
        void
    """

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

# For calculating the chemistry after each match
def calc_chemistry(player_chemistry, player_rating, prev_player_rating, team_player_dict):
    """
    calc_chemistry : The chemistry per player pair for a match

    Arguements:
        player_chemistry {dict} -- Dictionary of separate player chemistries
        player_rating {dict} -- Rating dictionary of player
        team_player_dict {dict} -- Dictionary of team id and players playing
    
    Returns:
        void
    """
    unique_teams = list(set(team_player_dict.keys()))

    # Compute chemistry for players of individual team
    # First for team 1
    for player_pair in itertools.combinations(team_player_dict[unique_teams[0]], 2):
        # Since these are for the same team , chemistry is updated as such
        p1,p2 = player_pair
        chem_change = 0

        p1_change = player_rating[p1] - prev_player_rating[p1]
        p2_change = player_rating[p2] - prev_player_rating[p2]

        # If ratings of both players either increases or decreases differently then we increase
        if ((p1_change < 0 and p2_change < 0) or (p1_change > 0 and p2_change > 0)):
            chem_change = (abs(p1_change) + abs(p2_change)) / 2
        # If ratings of both players increases or decreases same way , then we reduce
        elif ((p1_change < 0 and p2_change > 0) or (p1_change > 0 and p2_change < 0)):
            chem_change = -((abs(p1_change) + abs(p2_change)) / 2)
        
        # Updating the player chemistry required
        player_chemistry[player_pair] += chem_change
    
    # Next for team 2
    for player_pair in itertools.combinations(team_player_dict[unique_teams[1]], 2):
        # Since these are for the same team , chemistry is updated as such
        p1,p2 = player_pair
        chem_change = 0

        p1_change = player_rating[p1] - prev_player_rating[p1]
        p2_change = player_rating[p2] - prev_player_rating[p2]

        # If ratings of both players either increases or decreases differently then we increase
        if ((p1_change < 0 and p2_change < 0) or (p1_change > 0 and p2_change > 0)):
            chem_change = (abs(p1_change) + abs(p2_change)) / 2
        # If ratings of both players increases or decreases same way , then we reduce
        elif ((p1_change < 0 and p2_change > 0) or (p1_change > 0 and p2_change < 0)):
            chem_change = -((abs(p1_change) + abs(p2_change)) / 2)
        
        # Updating the player chemistry required
        player_chemistry[player_pair] += chem_change
    
    # Next for combined of teams 1 and teams 2
    combined_team_list = [(i,j) for i in team_player_dict[unique_teams[0]] for j in team_player_dict[unique_teams[1]]]
    for player_pair in combined_team_list:
        p1,p2 = player_pair
        
        p1_change = player_rating[p1] - prev_player_rating[p1]
        p2_change = player_rating[p2] - prev_player_rating[p2]

        # If ratings of both teams either increases or decreases differently then we increase
        if ((p1_change < 0 and p2_change > 0) or (p1_change > 0 and p2_change < 0)):
            chem_change = (abs(p1_change) + abs(p2_change)) / 2
        # If ratings of both teams increases or decreases same way , then we reduce
        elif ((p1_change > 0 and p2_change > 0) or (p1_change < 0 and p2_change < 0)):
            chem_change = -((abs(p1_change) + abs(p2_change)) / 2)
        
        # Updating the player chemistry required
        player_chemistry[player_pair] += chem_change



