from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg,count,min,max,sum,round
from utils.logger import logger

spark=None

try:
    logger.info('Creating SparkSession')
    spark=(
        SparkSession.builder
        .master('local[4]')
        .appName('aggregations practice')
        .getOrCreate()
    )
    
    logger.info('SparkSession created successfully')
    
    logger.info('Reading Employee_Complete_Dataset.csv file')
    
    df=spark.read.csv(
        'data/employees.csv',
        header=True,
        inferSchema=True
    )
    
    logger.info('Count employees in each department')
    
    df.groupBy('department').count().show()
    
    logger.info('avg salary for each department')
    
    df.groupBy(
        'department'
        ).avg('salary').show()
    
    logger.info('Max salary in each department')
    
    
    df.groupBy(
        'department'
        ).max('salary').show()
    
    logger.info('Min salary in each department')
    
    df.groupBy(
        'department'
    ).min('salary').show()
    
    logger.info('Total salary for each department')
    
    df.groupBy(
        'department'
    ).sum('salary').show()
    
    logger.info('Using Aggregations')

    df.groupBy('department').agg(
       count('*').alias('employee_count'),
       sum('salary').alias('total_salary'),
       avg('salary').alias('avg_salary'),
       min('salary').alias('min_salary'),
       max('salary').alias('max_salary')
   ).show()
    
    
    logger.info('Sort departments by highest average salary.')
    
    df.groupBy(
        'department'
    ).agg(
        avg('salary').alias('avg_salary')
    ).orderBy(col('avg_salary').desc())
    
    logger.info('Sort departments by highest total salary')
    
    
    df.groupBy(
        'department'
    ).agg(
        sum('salary').alias('total_salary')
        
    ).orderBy(col('total_salary').desc()).show()
    
    logger.info('Rename all aggregated columns using alias')
    
    df.groupBy(
        'department'
    ).agg(
        avg('salary').alias('average_salary'),
        sum('salary').alias('total_salary')
    ).show()
    
    logger.info('Round the average salary to 2 decimal places using round')
    
    df.groupBy('department').agg(
        round(avg('salary'),2).alias('average_salary')
    ).show()

    
    
    
except Exception as e:
    logger.exception(f'Application Failed ',{e})

finally:
    if spark:
        input('Press enter to stop')
        spark.stop()
        logger.info("Spark stopped")