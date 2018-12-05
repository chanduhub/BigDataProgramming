import os
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score

os.environ["SPARK_HOME"] = "C:\\spark\\"
os.environ["HADOOP_HOME"]="C:\\winutils"


from pyspark.python.pyspark.shell import spark

work_data = spark.read.load("Absenteeism_at_work.csv", format="csv", header=True, delimiter=",")

work_data = work_data.withColumn("roa", work_data["Reason for absence"] - 0).withColumn("label", work_data['Seasons'] - 0). \
    withColumn("bmi", work_data["Body mass index"] - 0). \
    withColumn("age", work_data["Age"] - 0). \
    withColumn("te", work_data["Transportation expense"] - 0)



assem = VectorAssembler(inputCols=["label", "roa"], outputCol='features')

work_data = assem.transform(work_data)

labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(work_data)


featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(work_data)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = work_data.randomSplit([0.7, 0.3])

# Train a RandomForest model.
rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10)

# Convert indexed labels back to original labels.
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)

y_true = work_data.select("bmi").rdd.flatMap(lambda x: x).collect()
y_pred = work_data.select("roa").rdd.flatMap(lambda x: x).collect()

# Chain indexers and forest in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")

accuracy = evaluator.evaluate(predictions)

confusionmatrix = confusion_matrix(y_true, y_pred)

precision = precision_score(y_true, y_pred, average='micro')

recall = recall_score(y_true, y_pred, average='micro')

rfModel = model.stages[2]
print(rfModel)

print("Test Accuracy = %g" % (accuracy))
print("Test Error = %g" % (1.0 - accuracy))

print("The Confusion Matrix  is :\n" + str(confusionmatrix))

print("The precision score  is: " + str(precision))

print("The recall score is: " + str(recall))