from nltk.corpus import stopwords
from string import punctuation
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

df = spark.read.json('/tmp/file.json')
# df = spark.read.json('s3://kureck/discourse/file.json')
stopwords_pt = set(stopwords.words('portuguese') + list(punctuation))

# remove null values from target because stringindexer fails with null values
df = df.na.drop(subset=['party'])

tokenizer = Tokenizer(inputCol="discourse", outputCol="words")

stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered").setStopWords(list(stopwords_pt))

countVectors = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=10000, minDF=5)

label_stringIdx = StringIndexer(inputCol="party", outputCol="label")

stages = [tokenizer, stopwordsRemover, countVectors, label_stringIdx]
pipeline = Pipeline(stages=stages)

print("Pipeline fit...")
pipelineFit = pipeline.fit(df)

print("Pipeline transform...")
dataset = pipelineFit.transform(df)
dataset.show(5)

print("Splitting in training and test dataset...")
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed=100)

print("Applying logistic regression...")
lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
lrModel = lr.fit(trainingData)

print("Making predicions...")
predictions = lrModel.transform(testData)
predictions.filter(predictions['prediction'] == 0) \
    .select("discourse", "party", "probability", "label", "prediction") \
    .orderBy("probability", ascending=False) \
    .show(n=10, truncate=30)

print("Evaluating model...")
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
print(evaluator.evaluate(predictions))

## TF-IDF
from pyspark.ml.feature import HashingTF, IDF
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)

idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms

pipeline = Pipeline(stages=[tokenizer, stopwordsRemover, hashingTF, idf, label_stringIdx])

print("Pipeline fit...")
pipelineFit = pipeline.fit(df)

print("Pipeline transform...")
dataset = pipelineFit.transform(df)

print("Splitting in training and test dataset...")
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)

print("Applying logistic regression...")
lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
lrModel = lr.fit(trainingData)

print("Making predicions...")
predictions = lrModel.transform(testData)
predictions.filter(predictions['prediction'] == 0) \
    .select("discourse", "party", "probability", "label", "prediction") \
    .orderBy("probability", ascending=False) \
    .show(n = 10, truncate = 30)

print("Evaluating model...")
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
print(evaluator.evaluate(predictions))

## With Cross-validation
pipeline = Pipeline(stages=[tokenizer, stopwordsRemover, countVectors, label_stringIdx])

print("Pipeline fit...")
pipelineFit = pipeline.fit(df)

print("Pipeline transform...")
dataset = pipelineFit.transform(df)

print("Splitting in training and test dataset...")
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)

print("Applying logistic regression...")
lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Create ParamGrid for Cross Validation
paramGrid = (ParamGridBuilder()
             .addGrid(lr.regParam, [0.1, 0.3, 0.5]) # regularization parameter
             .addGrid(lr.elasticNetParam, [0.0, 0.1, 0.2]) # Elastic Net Parameter (Ridge = 0)
#            .addGrid(model.maxIter, [10, 20, 50]) #Number of iterations
#            .addGrid(idf.numFeatures, [10, 100, 1000]) # Number of features
             .build())
# Create 5-fold CrossValidator

cv = CrossValidator(estimator=lr, \
                    estimatorParamMaps=paramGrid, \
                    evaluator=MulticlassClassificationEvaluator(), \
                    numFolds=5)
print("Applying cross CrossValidator...")
cvModel = cv.fit(trainingData)

predictions = cvModel.transform(testData)

# Evaluate best model
print("Evaluating model...")
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
print(evaluator.evaluate(predictions))

## Naive Bayes
from pyspark.ml.classification import NaiveBayes

nb = NaiveBayes(smoothing=1)
model = nb.fit(trainingData)

predictions = model.transform(testData)
predictions.filter(predictions['prediction'] == 0) \
    .select("discourse", "party", "probability", "label", "prediction") \
    .orderBy("probability", ascending=False) \
    .show(n = 10, truncate = 30)

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
evaluator.evaluate(predictions)

## Random Forest
from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(labelCol="label", \
                            featuresCol="features", \
                            numTrees = 100, \
                            maxDepth = 4, \
                            maxBins = 32)
# Train model with Training Data
rfModel = rf.fit(trainingData)
predictions = rfModel.transform(testData)
predictions.filter(predictions['prediction'] == 0) \
    .select("discourse", "party", "probability", "label", "prediction") \
    .orderBy("probability", ascending=False) \
    .show(n = 10, truncate = 30)

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
evaluator.evaluate(predictions)
