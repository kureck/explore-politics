from nltk.corpus import stopwords
from string import punctuation
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

df = spark.read.json('/tmp/file.json')
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
