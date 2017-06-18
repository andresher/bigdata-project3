import re, csv, string
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import rand
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF
from pyspark.ml.classification import LogisticRegression
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
from operator import add
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
from datetime import datetime, timedelta
try:
    import json
except ImportError:
    import simplejson as json
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11.jar pyspark-shell'

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config("spark.sql.warehouse.dir", '/user/hive/warehouse').enableHiveSupport().getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def consumer():
    context = StreamingContext(sc, 30) # Change to 300
    dStream = KafkaUtils.createDirectStream(context, ["trump"], {"metadata.broker.list": "localhost:9092"})
    dStream.foreachRDD(p1)
    context.start()
    context.awaitTermination()

def insertKeywords(text, spark, time):
    if text:
        rddKeywords = sc.parallelize(text)
        rddKeywords = rddKeywords.map(lambda x: x.lower())
        if rddKeywords.count() > 0:
            # Convert RDD[String] to RDD[Row] to DataFrame
            keywordDataFrame = spark.createDataFrame(rddKeywords.map(lambda x: Row(sentence=x, timestamp=time)))
            keywordDataFrame.createOrReplaceTempView("tweets")
            spark.sql("create database if not exists bdp3")
            spark.sql("use bdp3")
            keywordDataFrame.write.mode("append").saveAsTable("tweets")
            print("Inserted tweets")
    else:
        print("No tweets avaliable to insert into hive")

def updateKeywords(spark):
    try:
        keywordsDataFrame = spark.sql("select * from tweets")
        keywordsRDD = keywordsDataFrame.rdd
        keywordsRDD = keywordsRDD.filter(lambda x: x["timestamp"] > datetime.now() - timedelta(hours=1))
        keywordsDataFrame = spark.createDataFrame(keywordsRDD.map(lambda x: Row(sentence=x)))
        global lrModel
        lrResult = lrModel.transform(keywordsDataFrame)
        positiveCount = lrResult.where('prediction == 1').count()
        negativeCount = lrResult.where('prediction == 0').count()
        time = datetime.now()
        resultDict = {"positive": positiveCount, "negative": negativeCount, "timestamp": time}
        f = open('/home/andres.hernandez2/bigdata-project3/out/keywords.txt', 'a')
        f.write(str(resultDict))
        f.write("\n")
        f.close()
        print("Appended to file")
    except:
        print("Exception appending to file")
        pass

def p1(time,rdd):
    rdd = rdd.map(lambda x: json.loads(x[1]))
    records = rdd.collect() #Return a list with tweets
    spark = getSparkSessionInstance(rdd.context.getConf())

    insertKeywords(text, spark, time)
    global lastKwRefresh
    if datetime.now() > lastKwRefresh + timedelta(minutes=1): # Run each hour
        updateKeywords(spark)
        lastKwRefresh = datetime.now()

def processTweet(tweet):
    #Convert to lower case
    tweet = tweet.lower()
    #Convert www.* or https?://* to URL
    tweet = re.sub('((www\.[^\s]+)|(https?://[^\s]+))','URL',tweet)
    #Convert @username to AT_USER
    tweet = re.sub('@[^\s]+','AT_USER',tweet)
    #Remove additional white spaces
    tweet = re.sub('[\s]+', ' ', tweet)
    #Replace #word with word
    tweet = re.sub(r'#([^\s]+)', r'\1', tweet)
    #trim
    tweet = tweet.strip('\'"')
    return tweet

if __name__ == "__main__":
    global lastKwRefresh = None

    # Start training
    print("Training...")
    trainingData = "/home/andres/sad.txt" # data[3]=text data[1]=label
    # Define spark
    conf = SparkConf().setAppName("Sentiment").setMaster("spark://136.145.216.169:7077")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.config("spark.sql.warehouse.dir", '/user/hive/warehouse').enableHiveSupport().getOrCreate()
    data = sc.textFile(trainingData)
    header = data.first()
    rdd = data.filter(lambda row: row != header)
    r = rdd.mapPartitions(lambda x: csv.reader(x))
    r = r.map(lambda x: (processTweet(x[3]), int(x[1])))
    r = r.map(lambda x: Row(sentence=x[0], label=int(x[1])))
    df = spark.createDataFrame(r).orderBy(rand()).limit(500000)
    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="base_words")
    hashingTF = HashingTF(numFeatures=10000, inputCol="base_words", outputCol="features")
    lr = LogisticRegression(maxIter=10000, regParam=0.001, elasticNetParam=0.0001)
    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, lr])
    splits = df.randomSplit([0.6, 0.4], 223)
    trainSet = splits[0]
    testSet = splits[1]
    global lrModel = pipeline.fit(trainSet)

    # Start predicting
    print("Starting to read tweets...")
    lastKwRefresh = datetime.now()
    print("Startup at", datetime.now())
    sc = SparkContext(appName="ConsumerTRUMP")
    consumer()
