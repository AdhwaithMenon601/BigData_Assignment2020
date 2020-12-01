import sys
import json
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import *
from model import *


def prepare_dataframe(player_profile):
    sp_sess = SparkSession.builder.appName('Read_Data').getOrCreate()
    player_new_profile = []
    tempd = {}
    print(player_profile)
    r_schema = StructType([StructField('player_id', IntegerType(), True), StructField('fouls', IntegerType(), True), StructField('name', StringType(), True), StructField('goals', IntegerType(
    ), True), StructField('owngoals', IntegerType(), True), StructField('pass_acc', FloatType(), True), StructField('shots', FloatType(), True), StructField('matches', IntegerType(), True)])
    for i in player_profile:
        tempd = {}
        tempd["player_id"] = i
        temp = player_profile[i]
        tempd["fouls"] = temp[0]
        tempd["name"] = temp[1]
        tempd["goals"] = temp[2]
        tempd["owngoals"] = temp[3]
        tempd["pass_acc"] = float(temp[4])
        tempd["shots"] = float(temp[5])
        tempd["matches"] = temp[6]
        player_new_profile.append(tempd)
        print(tempd)
    player_df = sp_sess.createDataFrame(player_new_profile, r_schema)
    print("THE COUNT IS ", player_df.count())
    return player_df


def clustering(player_profile):
    vecAssembler = VectorAssembler(
        inputCols=["fouls", "goals", "owngoals", "pass_acc", "shots", "matches"], outputCol="features")
    print(player_profile.printSchema())
    player_profile = player_profile.drop("name")
    new_df = vecAssembler.transform(player_profile)
    kmeans = KMeans(k=5)
    model = kmeans.fit(new_df.select("features"))
    # Make predictions
    predictions = model.transform(new_df)
    predictions.show()
    # Evaluate clustering by computing Silhouette score
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    print("Silhouette with squared euclidean distance = " + str(silhouette))
    # Shows the result.
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)
    return predictions, centers


def predict(player_chem, player_profile, player_ratings, team1, team2):
    player_coeff1 = {}
    lessthan1 = []
    for i in team1:
        player = i
        team1.remove(i)
        total1 = 0
        if player_profile[i][6] >= 5:
            for j in team1:
                if (i, j) in player_chem:
                    total1 += player_chem[(i, j)]
                else:
                    total1 += player_chem[(j, i)]
            total1 = total1/len(team1)-1
            player_coeff1[i] = total1
        else:
            lessthan1.append(i)
    tot1 = 0
    for i in player_coeff1:
        tot1 += (player_coeff1[i]*find_rating(i, "2020-08-11"))
    player_coeff2 = {}
    lessthan2 = []
    for i in team2:
        player = i
        team2.remove(i)
        total2 = 0
        if player_profile[i][6] >= 5:
            for j in team2:
                if (i, j) in player_chem:
                    total2 += player_chem[(i, j)]
                else:
                    total2 += player_chem[(j, i)]
            total2 = total2/len(team2)-1
            player_coeff2[i] = total2
        else:
            lessthan2.append(i)
    tot2 = 0
    for i in player_coeff1:
        tot2 += (player_coeff1[i]*find_rating(i, "2020-08-11"))
    player_df = prepare_dataframe(player_profile)
    predictions, centers = clustering(player_df)
    for i in lessthan1:
        tempdf = predictions.filter(predictions["player_id"] == i)
        cluster_id = predictions.filter(
            predictions["prediction"] == tempdf["prediction"])
        temp_ids_df = cluster_id.select("player_id").collect()
        temp_ids1 = []
        for j in temp_ids_df:
            j_dict = j.asDict()
            temp_ids1.append(j_dict["player_id"])
        tot = 0
        for k in temp_ids1:
            if k in player_ratings and k in player_coeff1:
                tot += (player_ratings[k]*player_coeff1[k])
        tot = tot/len(temp_ids_df)
        tot1 += tot

    for i in lessthan2:
        tempdf = predictions.filter(predictions["player_id"] == i)
        cluster_id = predictions.filter(
            predictions["prediction"] == tempdf["prediction"])
        temp_ids_df = cluster_id.select("player_id").collect()
        temp_ids2 = []
        for j in temp_ids_df:
            j_dict = j.asDict()
            temp_ids2.append(j_dict["player_id"])
        tot = 0
        for k in temp_ids2:
            if k in player_ratings and k in player_coeff2:
                tot += (player_ratings[k]*player_coeff2[k])

        tot = tot/len(temp_ids_df)
        tot2 += tot
    strength_a = tot1/11
    strength_b = tot2/11
    chance_a = (0.5+strength_a-((strength_a+strength_b)/2))*100
    chance_b = 100-chance_a
    return chance_a, chance_b
