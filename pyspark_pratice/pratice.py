# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col

# spark=(
#     SparkSession.builder 
#     .master('local[3]')
#     .appName('practice filter')
#     .getOrCreate()
# )


# df=spark.read.csv(
#     'data/employees.csv',
#     header=True,
#     inferSchema=True 
# )

# print('******* salary ************')
# df.filter(
#     (col('salary')>55000)
# ).show()

# print('******* department IT ************')
# df.filter(
#     (col('department')=="IT")
# ).show()

# print('******* department HR or Finance ************')

# df.filter(
#     (col('department').isin('HR','Finance') )
# ).show()


# print('******* employees not in HR ************')

# df.filter(
#     (col('department') != 'HR')
# ).show()


# print('******* salary between 55000 and 75000 ************')

# df.filter(
#     (col('salary').between(55000,75000))
# ).show()

# print('******* startswith A ************')

# df.filter(
#     (col('name').startswith('A'))
# ).show()

# print('******* contains "ra" ************')

# df.filter(
#     (col('name').contains('ra'))
# ).show()


# print('******* Employees in IT with salary greater then 60,000 ************')

# df.filter(
#     (col('department')=='IT') &
#      (col('salary')>60000)
     
# ).show()


# print('******* HR or salary greater than 75000 ************')

# df.filter(
#     (col('department')=='HR') | 
#      (col('salary')>75000)
# ).show()


# print('******* isNull ************')

# df.filter(
#     (col('salary').isNull())
# ).show()


# print('******* isNotNull ************')

# df.filter(
#     (col('salary').isNotNull())
# ).show()







# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col


# spark=(
#     SparkSession.builder
#     .master('local[2]')
#     .appName('withColum')
#     .getOrCreate()
# )


# df=spark.read.csv(
#     'data/employees.csv',
#     header=True,
#     inferSchema=True
# )


# df.withColumn(
#     'annual_salary',
#     col('salary')*12
# ).show()


# df.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark=(
    SparkSession.builder
    .appName('explain ')
    .getOrCreate()
)

customers=spark.read.csv(
    'data/joins/customers.csv',
    header=True,
    inferSchema=True
)

orders=spark.read.csv(
    'data/joins/orders.csv',
    header=True,
    inferSchema=True
)

c = customers.alias("c")
o = orders.alias("o")
c.show()
o.show()
result=c.join(
    o,
    col("c.customer_id")==col("o.customer_id"),
    "inner"
)
result.show()
result.explain()