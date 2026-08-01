# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col

# spark=(
#     SparkSession.builder
#     .appName('select and col use cases')
#     .getOrCreate()
# )


# df=spark.read.csv(
#     'data/employees.csv',
#     header=True,
#     inferSchema=True
# )


# # df.select('name','salary').show()

# # df.select('*').show()

# df.select(df.name,df.salary).show()

# df.select(
#     col('name').alias('employee_name') ,
#     col('salary').alias('employee_monthly_salary')
# ).show()
# print('--'*25)
# df.select(
#     'name',
#     'salary',
#     (col('salary')*12).alias('annual_salary')
# ).show()
# input('press enter to stop')









from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.logger import logger

spark=(
    SparkSession.builder
    .master('local[2]')
    .appName("Select Practice")
    .getOrCreate()
)

logger.info('Reading employees.csv')

df=spark.read.csv(
    'data/employees.csv',
    # 'data/Employee_Complete_Dataset.csv',
    header=True,
    inferSchema=True
)

logger.info('Showing all data')
print('-----------All Data---------------')
df.show()

logger.info('Selecting name column')
df.select('name').show()

logger.info('Selecting name and Current_Salary')

df.select('name','salary').show()

logger.info('Renaming Columns')

df.select(
    col('name').alias('employee_name'),
    col('salary').alias('monthly_salary')
).show()

logger.info('Calculating annual Current_Salary')


df.select(
    col('name').alias('employee_name'),
    col('department'),
    col('salary').alias('monthly_salary'),
    (col('salary')*12).alias('annual_salary')
).show()

print('-*'*25)

print(spark.sparkContext.defaultParallelism)
# print(spark.sparkContext.getConf().getAll())
input('Press enter to stop')
spark.stop()