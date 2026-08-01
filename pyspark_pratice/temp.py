from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PartitionExample").getOrCreate()

sc = spark.sparkContext

data = [10, 20, 30, 40, 50, 60]

# Create RDD with 3 partitions
rdd = sc.parallelize(data, 3)

# Check number of partitions
print(rdd.getNumPartitions())

# View data
print(rdd.collect())