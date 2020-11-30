import sys
import json
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

def clustering(player_profile):
	kmeans = KMeans().setK(5).setSeed(1)
	model = kmeans.fit(player_profile)
	# Make predictions
	predictions = model.transform(dataset)
	# Evaluate clustering by computing Silhouette score
	evaluator = ClusteringEvaluator()
	silhouette = evaluator.evaluate(predictions)
	print("Silhouette with squared euclidean distance = " + str(silhouette))
	# Shows the result.
	centers = model.clusterCenters()
	print("Cluster Centers: ")
	for center in centers:
    		print(center)	
def predict(player_chem,player_profile,player_rating,team1,team2):
	player_coeff={}
	for i in team1:
		player=i
		temp=team1.remove(i)
		total1=0
		for j in temp:
			total1+=player_chem((i,j))
		total1=total1/len(team1)-1
		player_coeff[i]=total1
	for i in team2:
		player=i
		temp=team2.remove(i)
		total2=0
		for j in temp:
			total2+=player_chem((i,j))
		total2=total2/len(team1)-1
		player_coeff[i]=total2
	player_new_profile={}
	for i in player_profile:
		player_new_profile["player_id"]=i
		temp=player_profile[i]
		player_new_profile["fouls"]=temp[0]
		player_new_profile["name"]=temp[1]
		player_new_profile["goals"]=temp[2]
		player_new_profile["owngoals"]=temp[3]
		player_new_profile["pass_acc"]=temp[4]
		player_new_profile["shots"]=temp[5]
	player_df=spark.createDataFrame([player_new_profile])			
	
			
		
	
