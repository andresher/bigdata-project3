import re, csv, string
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import rand
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF
from pyspark.ml.classification import LogisticRegression

trainingData = "/home/andres/sad.txt" # data[3]=text data[1]=label

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

lrModel = pipeline.fit(trainSet)
lrResult = lrModel.transform(testSet)

avg = lrResult.where('label == prediction').count() / lrResult.count()
print(avg)
