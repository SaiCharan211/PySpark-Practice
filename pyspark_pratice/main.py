from pyspark.sql import SparkSession
from utils.logger import logger

spark=None

try:
    logger.info('Starting Spark Application')
    
    spark=(
        SparkSession.builder 
        .appName('Employee DataFrame Demo')
        .getOrCreate()
    )
    
    logger.info("Spark Session Created")
    
    df=spark.read.csv(
        'data/employees.csv',
        header=True,
        inferSchema=True
    )
    
    logger.info('Employees CSV loaded successfully')
    df.show()
    
except Exception as e:
    logger.exception(f'Application Failed:{e}')
    
finally:
    if spark:
        input("Press Enter to stop Spark...")
        spark.stop()
        logger.info("Spark Session Stopped")
        
        


