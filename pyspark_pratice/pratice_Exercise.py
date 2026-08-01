#  module 10
# sort, ace,desc

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from utils.logger import logger

# spark=None

# try:
#     logger.info('Creating Spark Session')
#     spark=(
#         SparkSession.builder
#         .master('local[2]')
#         .appName('Practice Module 10 sortBy')
#         .getOrCreate()
#     )
    
#     logger.info('Spark Session has been Created')
    
#     logger.info('Reading employees.csv Data Frame or file')
#     employees=spark.read.csv(
#         'data/employees.csv',
#         header=True,
#         inferSchema=True
#     )
    
#     logger.info('Sorting employees by salary (ascending)')
    
#     salary_asc=(
#         employees.orderBy(
#             col('salary').asc()
#         )
#     )
#     salary_asc.show()
    
#     logger.info('Sorting employees by salary (descending)')
    
#     salary_dsc=(
#         employees.orderBy(
#             col('salary').desc()
#         )
#     )
#     salary_dsc.show()
    
    
#     logger.info('Sorting employees by department (ascending).')
    
#     department_asc=(
#         employees.orderBy(col('department').asc())
#     )
    
#     department_asc.show()
    
    
#     logger.info('Sorting employees by department (ascending) and salary (descending).')
    
#     department_salary=(
#         employees.orderBy(
#             col('department').asc(),
#             col('salary').desc()
#         )
#     )
#     department_salary.show()
    
# except Exception as e:
#     logger.info('Application Failed')

# finally:
#     if spark:
#         input('Press Enter to stop Spark Session')
#         spark.stop()
#         logger.info("Spark Session has stopped")





# distinct(), dropDuplicate(), withColumnRenamed(),cast(),Drop(),

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col 
# from utils.logger import logger


# spark=None

# try: 
#     logger.info('Creating Spark Session')
#     spark=(
#         SparkSession.builder
#         .master('local[3]')
#         .appName('Module 10')
#         .getOrCreate()
#     )

#     logger.info('Spark Session Created')

#     logger.info('Reading employees.csv dataframe')
#     employees=spark.read.csv(
#         'data/employees.csv',
#         header=True,
#         inferSchema=True
#     )
#     logger.info('Original DataFrame')
#     employees.show()

#     logger.info('Print Schema')
#     employees.printSchema()

#     count=employees.count()
#     logger.info(f'Count original rows: {count}')

#     logger.info('Removing exact duplicates using distinct()')
#     distinct_df=employees.distinct()
#     distinct_df.show()

#     logger.info('Removing duplicate employee_id values.')

#     deduplicate_df=(
#         distinct_df.dropDuplicates(['employee_id'])
#     )
#     deduplicate_df.show()

#     logger.info('Renaming columns.')

#     renamed_df=(
#         deduplicate_df
#         .withColumnRenamed('name','employee_name')
#         .withColumnRenamed('department','department_name')
#     )
#     logger.info('Casting salary to double')

#     cast_df=(
#         renamed_df.withColumn(
#             'salary',
#             col('salary').cast('double')
#         )
#     )

#     logger.info('Dropping age column if present')

#     if 'age' in cast_df.columns:
#         final_df=cast_df.drop('age')
#     else:
#         final_df=cast_df 


#     logger.info('Sorting by department_name and salary descending.')

#     final_df=(
#         final_df.orderBy(
#             col('department_name').asc(),
#             col('salary').desc()
#         )
#     )
    
#     final_df.show()

#     logger.info('Final DataFrame')

#     final_df.show()
#     logger.info('Final Schema')

#     final_df.printSchema()

#     final_count=final_df.count()

#     logger.info(f'Final row Count: {final_count}')
    
# except Exception as e:
#     logger.info(f"Application Failed: {e}")

# finally:
#     if spark:
#         input('Press Enter to Stop the Session')
#         spark.stop()
#         logger.info('Spark Session has been Stopped')
        




from pyspark.sql import SparkSession
from pyspark.sql.functions import col 
from utils.logger import logger


spark=None

try: 
    logger.info('Creating Spark Session')
    spark=(
        SparkSession.builder
        .master('local[3]')
        .appName('Module 10')
        .getOrCreate()
    )

    logger.info('Spark Session Created')

    logger.info('Reading employees.csv dataframe')
    employees=spark.read.csv(
        'data/employees.csv',
        header=True,
        inferSchema=True
    )
    logger.info('Original DataFrame')
    employees.show()
    
    logger.info('Original Schema')
    employees.printSchema()
    
    original_count=employees.count()
    logger.info(f"Original row count:  {original_count}")
    
    logger.info('Applying DataFrame transformations.')
    
    final_df=(
        employees
        .distinct()
        .dropDuplicates(['employee_id'])
        .withColumnRenamed("name", "employee_name")
        .withColumnRenamed("department", "department_name")
        .withColumn(
            'salary',
            col('salary').cast('double')
        )
        .drop('age')
        .orderBy(
            col('department_name').asc(),
            col('salary').desc()
        )
    )
    
    logger.info('Final DataFrame')
    final_df.show()
    
    logger.info("Final Schema")
    final_df.printSchema()
    
    final_count = final_df.count()
    logger.info(f"Final row count: {final_count}")
    
except Exception as e:
    logger.info(f"Application Failed: {e}")

finally:
    if spark:
        input('Press Enter to Stop the Session')
        spark.stop()
        logger.info('Spark Session has been Stopped')