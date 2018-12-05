import os
from pyspark.ml.feature import VectorAssembler
from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score

# setting SPARK and HADOOP HOME
os.environ["SPARK_HOME"] = "C:\\spark\\"
os.environ["HADOOP_HOME"]="C:\\winutils"

from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession


# creating spark variable from SparkSession
spark = SparkSession.builder.getOrCreate()
# Loading csv file
work_data = spark.read.load("Absenteeism_at_work.csv", format="csv", header=True, delimiter=",")

work_data = work_data.withColumn("roa", work_data["Reason for absence"] - 0).withColumn("label", work_data['Seasons'] - 0). \
    withColumn("bmi", work_data["Body mass index"] - 0). \
    withColumn("age", work_data["Age"] - 0). \
    withColumn("te", work_data["Transportation expense"] - 0)

assem = VectorAssembler(inputCols=["label", "roa"], outputCol='features')

work_data = assem.transform(work_data)
# Split the data into train and test
splits = work_data.randomSplit([0.7, 0.3], 1000)
train = splits[0]
test = splits[1]

# create the trainer and set its parameters
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

# train the model
model = nb.fit(train)

# select example rows to display.
predictions = model.transform(test)

# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")

y_true = work_data.select("bmi").rdd.flatMap(lambda x: x).collect()
y_pred = work_data.select("roa").rdd.flatMap(lambda x: x).collect()


accuracy = evaluator.evaluate(predictions)

confusionmatrix = confusion_matrix(y_true, y_pred)

precision = precision_score(y_true, y_pred, average='micro')

recall = recall_score(y_true, y_pred, average='micro')


print("Test set accuracy = " + str(accuracy))

print("The Confusion Matrix  is :\n" + str(confusionmatrix))

print("The precision score  is: " + str(precision))

print("The recall score is: " + str(recall))