# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from utils.logger import logger

# spark=None

# try:
#     logger.info('Creating Spark Session')
    
#     spark=(
#         SparkSession.builder
#         .master('local[3]')
#         .appName('Reading and Writing Files')
#         .getOrCreate()
#     )
#     logger.info('Reading employee.csv')
    
#     employees=spark.read.csv(
#         'data/employees.csv',
#         header=True,
#         inferSchema=True
#     )
    
#     logger.info('Display Schema')
#     employees.printSchema()
    
#     logger.info('Employee Count')
#     print('Count:', employees.count())
    
#     logger.info('show() 10 rows')
#     employees.show(10)
    
#     logger.info('first 10')
#     r=employees.first()
#     print(r)
    
    
#     logger.info('Writing CSV')
    
#     file=(
#         employees.write
#         .format('csv')
#         .mode('overwrite')
#         .option('header',True)
#         .save('output/employees_csv')
#     )
    
#     logger.info('writing JSON')
#     (
#         employees.write
#         .format('json')
#         .mode('overwrite')
#         .save('output/employees_payment')
#     )
    
    
# except Exception as e:
#     logger.info(f'Application Failed {e}')

# finally:
#     if spark:
#         input('Press Enter to Stop Session')
#         spark.stop()
#         logger.info('Spark Session is Stopped')





import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"
from pyspark.sql import SparkSession


# Create Spark session
spark = (
    SparkSession.builder
    .appName("TestSpark")
    .master("local[*]")
    .getOrCreate()
)

print("✅ Spark started successfully")

# Check Spark version
print("Spark Version:", spark.version)

# Check Hadoop version
print(
    "Hadoop Version:",
    spark.sparkContext._jvm
    .org.apache.hadoop.util.VersionInfo
    .getVersion()
)


# Create a small DataFrame
data = [
    (1, "Sai"),
    (2, "John"),
    (3, "David")
]

columns = ["id", "name"]

df = spark.createDataFrame(data, columns)


print("\nShowing DataFrame:")
df.show()


# Test writing a file
print("\nWriting CSV...")

df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(r"E:\PySpark\test_output")


print("✅ CSV written successfully")


# Test reading the file
print("\nReading CSV...")

new_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(r"E:\PySpark\test_output")
)

new_df.show()

print("✅ Read operation successful")


spark.stop()

print("✅ Spark stopped")