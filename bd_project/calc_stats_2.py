import sys
import json
import itertools
import gc
from pyspark.sql import functions as func
from pyspark.sql.types import BooleanType

# returns player contribution for players fo each team


def player_contribution_dict(value, dtime, player_contribution, players_pass, players_duel, players_kick, players_shot):
    """
    duration=match_df.collect()[0].duration
    if duration=="Regular":
            dtime=90
    else:
            dtime=90+30
    team_data=match_df.select("teamsData").collect()[0].asDict().values()
    """
    # print(team_data)
    # print(value)
    value = value["formation"]
    subs = value["substitutions"]
    subs_out = {}
    subs_in = {}
    for i in subs:
        subs_out[i["playerOut"]] = i["minute"]
        subs_in[i["playerIn"]] = i["minute"]
    line_up = value["lineup"]
    players = {}
    for i in line_up:
        pid = i["playerId"]
        if pid not in players:
            if pid not in subs_out:
                players[pid] = dtime
            else:
                players[pid] = subs_out[pid]
    for i in subs_in:
        if i not in players:
            players[i] = dtime-subs_in[i]
    # players_contribution={1:1,2:2,3:3}
    for i in players:
        cont = 0
        if i in players_pass:
            cont += players_pass[i]
        if i in players_duel:
            cont += players_duel[i]
        if i in players_kick:
            cont += players_kick[i]
        if i in players_shot:
            cont += players_shot[i]
        cont = cont/4
        if players[i] == dtime:
            cont *= 1.05
        else:
            cont *= (players[i]/90)
        player_contribution[i] = cont
    return player_contribution

# returns player contribution for all players


def player_contribution_main(match_df, players_pass, players_duel, players_kick, players_shot):
    duration = match_df["duration"]
    if duration == "Regular":
        dtime = 90
    else:
        dtime = 90+30
    team_data = match_df["teamsData"].values()
    player_contribution = {}
    for i in team_data:
        player_contribution_dict(i, dtime, player_contribution,
                                 players_pass, players_duel, players_kick, players_shot)
    return player_contribution


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
        player_perf = player_contribution[i] - \
            ((0.005 * fouls_per_player[i]) + (0.5 * own_per_player[i]))
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
        p1, p2 = player_pair
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
        p1, p2 = player_pair
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
    combined_team_list = [(i, j) for i in team_player_dict[unique_teams[0]]
                          for j in team_player_dict[unique_teams[1]]]
    for player_pair in combined_team_list:
        p1, p2 = player_pair

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


def ret_players(match_df):

    team_id = list(match_df['teamsData'].keys())
    teams_dict = {}
    
    # Bench information for the team
    bench_1 = match_df['teamsData'][team_id[0]]['formation']['bench']
    bench_2 = match_df['teamsData'][team_id[1]]['formation']['bench']
    bench_id_1 = [id['playerId'] for id in bench_1]
    bench_id_2 = [id['playerId'] for id in bench_2]

    # Lineup information for the teams
    lineup_1 = match_df['teamsData'][team_id[0]]['formation']['lineup']
    lineup_2 = match_df['teamsData'][team_id[1]]['formation']['lineup']
    lineup_id_1 = [player['playerId'] for player in lineup_1]
    lineup_id_2 = [player['playerId'] for player in lineup_2]

    # Team players for each team
    team_1 = bench_id_1 + lineup_id_1
    team_2 = bench_id_2 + lineup_id_2
    teams_dict[team_id[0]] = team_1
    teams_dict[team_id[1]] = team_2
    
    return teams_dict
