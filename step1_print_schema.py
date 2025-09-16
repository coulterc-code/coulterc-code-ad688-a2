from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("JobPostingsAnalysis").getOrCreate()
df=spark.read.option("header","true").option("inferSchema","true").option("multiLine","true").option("escape","\"").csv("data/lightcast_job_postings.csv")
df.printSchema()
