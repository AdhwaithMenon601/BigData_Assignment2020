import sys
import json
import itertools
import gc
from pyspark.sql import functions as func
from pyspark.sql.types import BooleanType

# Initialising the chemistry
def init_chemistry(players):
    """
    init_chemistry : Initialises the chemistry for all players

    Arguements:
        players {df} -- Dataframe of all the players
    
    Returns:
        dict -- player chemistry per player pair
    """
    # Converts the player id from the player df into a list of IDs
    player_list = [i['Id'] for i in players.rdd.collect()]
    player_chemistry = {}
    # The above is useful when we need to prepare the initial chemistry
    for chem_pair in itertools.combinations(player_list, 2):
        chem_pair = set(chem_pair)
        player_chemistry[chem_pair] = 0.5
    
    return player_chemistry

# Initialising the ratings
def init_ratings(players):
    """
    init_ratings : Initialises the ratings for all players

    Arguements:
        players {df} -- Dataframe of all the players
    
    Returns:
        dict -- player ratings per player pair
    """
    # Converts the player id from the player df into a list of IDs
    player_list = [i['Id'] for i in players.rdd.collect()]
    player_ratings = {}
    # The above is useful when we need to prepare the initial chemistry
    for player in player_list:
        player_ratings[player] = 0.5
    
    return player_ratings