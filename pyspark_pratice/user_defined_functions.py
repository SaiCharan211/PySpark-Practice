from pyspark.sql import SparkSession
from pyspark.sql.functions import col,udf,lit
from pyspark.sql.types import StringType,BooleanType,DoubleType
from utils.logger import logger

spark=None

try:
    logger.info('Creating Spark Session')
    
    spark=(
        SparkSession.builder
        .master('local[3]')
        .appName('Module 14: User Defined Function')
        .getOrCreate()
    )
    
    logger.info('Spark Session Created')
    
    employees = (
        spark.read
        .csv(
            "data/employees.csv",
            header=True,
            inferSchema=True
        )
    )
    
    employees.show()
    employees.printSchema()

except Exception as e:
    logger.exception(
        "Application Failed"
    )


finally:

    if spark:

        input(
            "Press Enter to Stop Spark Session"
        )

        spark.stop()

        logger.info(
            "Spark Session Stopped"
        )