import sys
import json
import gc
from pyspark.sql import functions as func
from pyspark.sql.types import BooleanType


def pass_accuracy(event_df):
    """
    pass_accuracy : Calculates Pass Accuracy per player

    Arguments:
        event_df {DF} -- Events Dataframe

    Returns:
        dict -- Accuracy per player
    """

    # Dicts for the Pass types
    key_pass = {'id': 302}
    acc_pass = {'id': 1801}
    inc_pass = {'id': 1802}

    # Selecting all pass events
    pass_df = event_df.filter(event_df['eventId'] == 8)
    player_stats = {}

    for record in pass_df.collect():

        if record['playerId'] not in player_stats.keys():
            player_stats.update({record['playerId']: [0, 0, 0, 0]})

        old_val = player_stats[record['playerId']]

        # Checking for accurate key pass
        if key_pass in record['tags'] and acc_pass in record['tags']:
            old_val[0] += 1

        # Checking for accurate pass
        elif acc_pass in record['tags'] and key_pass not in record['tags']:
            old_val[1] += 1

        # Checking for inaccurate key pass
        elif key_pass in record['tags'] and acc_pass not in record['tags']:
            old_val[2] += 1

        # Else normal inaccurate pass
        elif key_pass not in record['tags'] and acc_pass not in record['tags']:
            old_val[3] += 1

        player_stats.update({record['playerId']: old_val})

    # Calculating accuracy for each player
    for player in player_stats:
        stats = player_stats[player]
        acc = (stats[1] + (stats[0] * 2)) / \
            ((stats[1] + stats[3]) + (stats[1] + stats[3]))
        player_stats.update({player: acc})

    return player_stats


def duel_effectiveness(event_df):
    """
    duel_effectiveness : Calculates duel effectiveness per player

    Arguments:
        event_df {DF} -- Pyspark DF for events

    Returns:
        dict -- effectiveness per player
    """

    # Selecting all duels
    duel_rdd = event_df.filter(event_df['eventId'] == 1)
    row_list = duel_rdd.select("playerId", "tags").collect()

    # List of duel events
    valid_list = [701, 702, 703]
    players_duel = {}

    # Iterating over DF rows
    for i in row_list:

        player_id = i.playerId
        tags = i.tags

        # If key not found, initialize
        if player_id not in players_duel:
            players_duel[player_id] = {701: 0, 702: 0, 703: 0}

        # Indexing inner dict
        for j in tags:
            if j['id'] in valid_list:
                players_duel[player_id][j['id']] += 1

    # Calculating duel effectiveness for each player
    for i in players_duel:
        stats = players_duel[i]
        players_duel[i] = (stats[703] + (stats[701] * 0.5)) / \
            (stats[701] + stats[702] + stats[703])

    return players_duel


def freekick_effectiveness(df):
    """
    freekick_effectiveness : Calculates freekick effectiveness per player

    Arguments:
        df {DF} -- Pyspark DF for events

    Returns:
        dict -- Effectiveness per player
    """
    free_kick = {}

    # Selecting required free kicks
    free_df = df.filter(df['eventid'] == 3)
    row = free_df.select("playerId", "tags", "subEventId").collect()

    # Above has all the required data that is needed
    for i in row:

        player_id = i.playerId
        tags = i.tags

        # If player not found, initialize
        if (not(player_id in free_kick)):
            free_kick[player_id] = [0, 0, 0, 0]

        for j in tags:
            free_kick[player_id][3] += 1

            # Accurate free kick
            if (j['id'] == 1801):
                free_kick[player_id][1] += 1

            # Penalty and a goal
            if (i.subEventId == 35 and j['id'] == 101):
                free_kick[player_id][2] += 1

        # If ever this is encountered
        if (free_kick[player_id][3] == 0):
            continue

        # Calculating the effectiveness
        free_kick[player_id][0] = (free_kick[player_id][1] + free_kick[player_id][2]) / \
            free_kick[player_id][3]

    return free_kick

def shots_effectiveness(event_df):
    """
    shots_effectiveness : Shots effectiveness for each player

    Arguments:
        event_df {DF} -- Pyspark DF for events

    Returns:
        dict -- Effectiveness per player
    """
    player_shots = {}

    # Event dicts
    on_target = {'id' : 1801}
    non_target = {'id' : 1802}
    goal = {'id' : 101}

    # Selecting all shots from the DF
    shots = event_df.filter(event_df['eventId'] == 10)
    for record in shots.collect():

        if record['playerId'] not in player_shots.keys():
            player_shots.update({record['playerId']: [0, 0, 0]})

        old_val = player_shots[record['playerId']]

        # Shot on target and a goal
        if on_target in record['tags'] and goal in record['tags']:
            old_val[0] += 1

        # Shot on target but not a goal
        elif on_target in record['tags'] and goal not in record['tags']:
            old_val[1] += 1

        # Shot not on target
        elif non_target not in record['tags']:
            old_val[2] += 1

        player_shots.update({record['playerId']: old_val})

    # Shots effectiveness for each player
    for player in player_shots:
        stats = player_shots[player]
        if (stats[0] + stats[1] + stats[2] == 0):
            eff = 0
        else:
            eff = (stats[0] + (stats[1] * 0.5)) / (stats[0] + stats[1] + stats[2])
        player_shots.update({player: eff})

    return player_shots


def fouls_loss(event_df):
    """
    fouls_loss : No of fouls per player

    Arguments:
        event_df {DF} -- Pyspark DF for events

    Returns:
        dict -- Fouls / player
    """
    fouls_per_player = {}

    # Filterting the foul events
    fouls_df = event_df.filter(event_df['eventId'] == 2)
    rows = fouls_df.select("playerId", "tags").collect()

    # If RDD is empty
    if (len(rows) == 0):
        return fouls_per_player

    # Iterating over rows
    for i in rows:
        player_id = i.playerId

        # If key doesnt exist, initialize
        if (not(player_id in fouls_per_player)):
            fouls_per_player[player_id] = 1

        # If key exists, update
        else:
            fouls_per_player[player_id] += 1

    return fouls_per_player


def own_goal(event_df):
    """
    own_goal : Own goals per player

    Arguments:
        event_df {DF} -- Pyspark DF for events

    Returns:
        dict -- Own goals / player
    """
    own_per_player = {}

    # Selecting player ID and tags
    rows = event_df.select("playerId", "tags").collect()
    for i in rows:
        player_id = i.playerId
        own_count = 0

        # If key doesnt exist, initialize
        if (not(player_id in own_per_player)):
            own_per_player[player_id] = 0

        # Checking for 102 : Own goal
        for j in i.tags:
            if (j['id'] == 102):
                own_per_player[player_id] += 1

    return own_per_player

          
    
