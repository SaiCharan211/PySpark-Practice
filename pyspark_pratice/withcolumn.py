from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,upper,lower,concat,when,current_date,current_timestamp
from utils.logger import logger

logger.info('Starting Spark Session')

spark=(
    SparkSession.builder
    .master('local[3]')
    .appName('withColum')
    .getOrCreate()
)
logger.info('Spark Session Started')

logger.info('Reading employees.csv file')

df=spark.read.csv(
    'data/employees.csv',
    header=True,
    inferSchema=True
)

logger.info('create new annual column using withColumn()')

df.withColumn(
    'annual_salary',
    col('salary')* 12
).show()

logger.info('create new bonus column using withColumn()')

df.withColumn(
    'bonus', col('salary')*0.15
).show()


logger.info('Create default value column')

df=(
    df
    .withColumn("country", lit("India"))
    .withColumn("continent", lit("Asia"))
)
df.show()

logger.info('Add company="OpenAI" ')

df.withColumn(
    'company',
    lit('OpenAI')
).show()


logger.info('Replacing salary with salary+5000')

df.withColumn(
    'salary',
    col('salary')+5000
).show()

logger.info('String Functions covert name to uppercase')

df.withColumn(
    'name',
    upper('name')
).show()

logger.info('String Functions covert name to lowercase')

df.withColumn(
    'department',
    lower('department')
).show()

logger.info('String Functions concat employee_info= name+ "__"+department ')

df.withColumn(
    'employee_info',
    concat(
        col('name'),
        lit('__'),
        col('department')
    )
).show()


logger.info('Multiple Conditions')

df.withColumn(
    'grade',
    when(col('salary')>=75000,'A')
    .when(col('salary')>=60000,'B')
    .otherwise('C')
).show()

logger.info('Date Functions load_date')

df.withColumn(
    'load_date',
    current_date()
).show()

logger.info('Date Functions load_timestamp')

df.withColumn(
    'load_timeStamp',
    current_timestamp()
).show()

