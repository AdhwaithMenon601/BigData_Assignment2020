import sys
import json
import itertools
import gc
from pyspark.sql import functions as func
from pyspark.ml.linalg import Vectors 
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression 

# Code for linear regression model
def make_model(player_regr):
    assembler = VectorAssembler(inputCols = ['diff'],outputCol = 'features')

    check_data = player_regr.select("diff","rating")

    train,test = check_data.randomSplit([0.7,0.3])
    train_df = assembler.transform(train)
    test_df = assembler.transform(test)

    lr = LinearRegression(featuresCol='features', labelCol='rating')
    final_model = lr.fit(train_df)

    res = final_model.evaluate(train_df)
    res.predictions.show()
    res = final_model.evaluate(test_df)
    res.predictions.show()