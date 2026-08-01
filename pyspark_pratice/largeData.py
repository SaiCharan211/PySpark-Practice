from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.logger import logger

spark = None

try:
    logger.info("Starting Spark Application")

    spark = (
        SparkSession.builder
        .appName("Large Data Demo")
        .getOrCreate()
    )

    logger.info("Spark Session Created")

    # Create 10 million records
    logger.info("Creating large DataFrame")

    df = spark.range(10000000)

    logger.info("DataFrame Created Successfully")

    # Check partitions
    partitions = df.rdd.getNumPartitions()

    logger.info(f"Number of partitions: {partitions}")

    # Transformation
    logger.info("Applying filter transformation")

    filtered_df = df.filter(col("id") > 5000000)

    logger.info("Transformation completed (not executed yet)")

    # Action
    logger.info("Starting action: count()")

    count = filtered_df.count()

    logger.info(f"Action completed. Count: {count}")

    # Keep Spark running for Spark UI
    input("Open Spark UI (http://localhost:4040) and press Enter to stop...")

except Exception as e:
    logger.exception(f"Application Failed: {e}")

finally:
    if spark:
        spark.stop()
        logger.info("Spark Session Stopped")
        
        
        


