from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, asc

spark = SparkSession.builder.appName("sorting").getOrCreate()
spark.sparkContext.setLogLevel('OFF')
logger = spark._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.OFF)

df = spark.read.csv("hdfs://main:9000/data/caps.csv", inferSchema=True, header=False).toDF("year", "serial_number")

result = df.filter(df["year"] <= 2023).orderBy(desc("year"), asc("serial_number"))

with open("./output.txt", 'w') as f:
    for row in result.collect():
        f.write(str(row['year']) + "," + row['serial_number'] + "\n")

spark.stop()