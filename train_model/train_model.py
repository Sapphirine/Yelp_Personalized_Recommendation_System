import math
from pyspark import SparkContext

# $example on$
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
# $example off$
sc = SparkContext(appName="PythonCollaborativeFiltering")

# Load the complete dataset file
data = sc.textFile("/Users/xx/Desktop/big_data/project/Yelp-Personalized-Recommendation/flask/yelp_reviews.csv")
header = data.take(1)[0]

# Parse
data = data.filter(lambda line: line!=header).map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

training_RDD, test_RDD = data.randomSplit([7, 3], seed=0L)

# build model
rank = 35
seed = 5L
iterations = 25
regularization_parameter = 0.4

model = ALS.train(training_RDD, rank, seed=seed, iterations=iterations, lambda_=regularization_parameter)

# test model
test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))
predictions = model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())

print 'For testing data the RMSE is %s' % (error)


model.save(sc, "/Users/xx/Desktop/big_data/project/Yelp-Personalized-Recommendation/flask/dataset/myCollaborativeFilter")
#sameModel = MatrixFactorizationModel.load(sc, "/tmp/data/myCollaborativeFilter")
