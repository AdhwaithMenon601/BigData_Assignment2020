import sys
import json
import itertools
import gc
from pyspark.sql import functions as func
from pyspark.ml.linalg import Vectors 
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import array, lit, datediff, col
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime

# Find rating using regression
def find_rating(player_id, cur_date):
    sp_sess = SparkSession.builder.appName('Regr_Data').getOrCreate()
    play_path = "hdfs://localhost:9000/players.csv"
    players = sp_sess.read.csv(play_path, header=True, inferSchema=True)
    assembler = VectorAssembler(inputCols = ['diff'],outputCol = 'features')
    name_df = players.filter(players['Id'] == player_id)
    player_date = name_df.select("birthDate").collect()[0].birthDate

    new_date1 = player_date.split('-')
    new_date2 = player_date.split('-')

    d1 = datetime.date(int(new_date1[0]), int(new_date1[1]), int(new_date1[2]))
    d2 = datetime.date(int(new_date2[0]), int(new_date2[1]), int(new_date2[2]))

    diff = abs(d2 - d1).days
    my_rating = 1.000
    my_schema = StructType([
        StructField('diff', IntegerType(), True),
        StructField('rating', FloatType(), True)
    ])
    my_dict = {'diff': diff, 'rating': my_rating}

    new_df = sp_sess.createDataFrame([my_dict], my_schema)

    test = assembler.transform(new_df)
    final_model = LinearRegressionModel.load('reg_model')

    res = final_model.evaluate(test)

    req = res.predictions.select("prediction").rdd.flatMap(lambda x : x).collect()

    return req[0]

# Code for linear regression model
def make_model(player_regr, initial_run):
    assembler_1 = VectorAssembler(inputCols = ['diff'],outputCol = 'features')
    lr = None
    train_df = None
    test_df = None

    # For non-initial run of model
    if initial_run == False:

        # Setting Old Model's weights
        old_model = LinearRegressionModel.load('reg_model')
        player_regr = player_regr.withColumn('weights', lit(old_model.coefficients[0]))
        check_data = player_regr.select("diff", "rating", 'weights')

        # Splitting the data
        train,test = check_data.randomSplit([0.7,0.3])

        # Transforming the data
        train_df = assembler_1.transform(train)
        test_df = assembler_1.transform(test)

        # LR Object
        lr = LinearRegression(featuresCol='features', labelCol='rating', weightCol='weights')

    else:
        check_data = player_regr.select("diff", "rating")

        # Splitting the data
        train,test = check_data.randomSplit([0.7,0.3])

        # Transforming the data
        train_df = assembler_1.transform(train)
        test_df = assembler_1.transform(test)

        # LR Object
        lr = LinearRegression(featuresCol='features', labelCol='rating')

    final_model = lr.fit(train_df)
    res = final_model.evaluate(train_df)
    res.predictions.show()
    res = final_model.evaluate(test_df)
    res.predictions.show()

    # Writing the model to file
    final_model.write().overwrite().save('reg_model')
