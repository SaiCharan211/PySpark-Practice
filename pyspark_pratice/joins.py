# Inner Join

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from utils.logger import logger

# spark=None

# try:
#     logger.info('Creating Spark Session')
#     spark=(
#     SparkSession.builder
#     .appName('Joins')
#     .getOrCreate()
#     )
    
#     logger.info('Spark Session Created')
    
#     logger.info('Reading customers.csv file')

#     customers=spark.read.csv(
#         'data/joins/customers.csv',
#         header=True,
#         inferSchema=True
#     )
    
#     logger.info('Reading orders.csv file')

#     orders=spark.read.csv(
#         'data/joins/orders.csv',
#         header=True,
#         inferSchema=True
#     )
    
#     logger.info('Showing customers and order dataframe or csv file')
#     customers.show()
#     orders.show()

#     logger.info('Joining customers and order dataframe')
    
#     result=customers.join(
#         orders,
#         customers.customer_id==orders.customer_id,
#         'inner'
#     )
    
#     logger.info('showing the results after join without removing duplicate columns')
#     result.show()

#     logger.info('Removing Duplicate columns using select()')
    
#     result=customers.alias('c').join(
#         orders.alias('o'),
#         customers.customer_id==orders.customer_id,
#         'inner'
#     ).select(
#         col('c.customer_id'),
#         col('c.customer_name'),
#         col('o.order_id'),
#         col('o.amount')
#     ).show()
     
    
# except Exception as e:
#     logger.exception('Application Failed ', {e})

# finally:
#     if spark:
#         input('Press Enter to stop')
#         spark.stop()
#         logger.info('Spark Stopped')
    


# Left Join

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from utils.logger import logger

# spark=None

# try:
#     logger.info('Creating Spark Session')
    
#     spark=(
#         SparkSession.builder
#         .master('local[3]')
#         .appName('Left Join')
#         .getOrCreate()
#     )
    
#     logger.info('Spark Session created')
    
#     logger.info('Reading customers.csv file')
#     customers=spark.read.csv(
#         'data/joins/customers.csv',
#         header=True,
#         inferSchema=True
#     )
    
#     logger.info('Reading orders.csv file')
#     orders=spark.read.csv(
#         'data/joins/orders.csv',
#         header=True,
#         inferSchema=True
#     )
    
    
#     logger.info('Creating alias for customers and orders dataframe')
#     c=customers.alias('c')
#     o=orders.alias('o')
    
#     logger.info('Setting Up left join for customers and orders dataframe')
    
#     left_join_df=(
#         c.join(
#             o,
#             col('c.customer_id')==col('o.customer_id'),
#             'left'
#         )
#         .select(
#             col('c.customer_id'),
#             col('c.customer_name'),
#             col('c.city'),
#             col('o.order_id'),
#             col('o.amount')
#         )
#     )
    
#     logger.info(' result of left join')
    
#     left_join_df.show()
    
#     logger.info('Find customers who have never placed an order')
    
#     customers_without_orders = left_join_df.filter(
#     col("order_id").isNull()
# )
#     customers_without_orders.show()
    
#     logger.info('Count how many customer have never placed an order')
#     count = customers_without_orders.count()
    
#     logger.info(f'Customer without orders: {count}' )
    
    
# except Exception as e:
#     logger.info(f"Application Failed {e}")

# finally:
#     if spark:
#             input('Press Enter to Stop Session')
#             spark.stop()
#             logger.info('Session Stopped')




# Right Join

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from utils.logger import logger

# spark=None

# try:
#     logger.info("Starting Spark Session")
#     spark=(
#         SparkSession.builder
#         .master('local[2]')
#         .appName('Right Join')
#         .getOrCreate()
#     )
    
#     logger.info('Spark Session Created')
    
#     logger.info('Reading customers.csv dataframe or file')
#     customers=spark.read.csv(
#         'data/joins/customers.csv',
#         header=True,
#         inferSchema=True
#     )
    
#     logger.info('Reading orders.csv dataframe or file')
#     orders=spark.read.csv(
#         'data/joins/orders.csv',
#         header=True,
#         inferSchema=True
#     )
    
#     logger.info('Creating alias for customers and orders dataframe')
#     c=customers.alias('c')
#     o=orders.alias('o')
    
#     logger.info('Performing Right Join')
    
#     right_join_df=(
#         c.join(
#             o,
#             col('c.customer_id')==col('o.customer_id'),
#             'right'
#         )
#         .select(
#             col('c.customer_id'),
#             col("c.customer_name"),
#             col("c.city"),
#             col("o.order_id"),
#             col("o.amount")
#         )
#     )
    
#     logger.info('Results after performing right join')
#     right_join_df.show()
    
#     customers_without_order=right_join_df.filter(
#         col('customer_id').isNull()
#     )
#     customers_without_order.show()
    
#     logger.info('count of unmatched orders')
    
#     count=customers_without_order.count()
#     logger.info(f'Count of unmatched rows: {count}'
#                     )
    
    
#     orderById= customers_without_order.orderBy(
#         col('order_id')
#     )
#     logger.info('Sort by order id')
#     orderById.show()
    
    
    
# except Exception as e:
#     logger.info(f'Application Failed: {e}')

# finally:
#     if spark:
#         input('Press Enter to stop')
#         spark.stop()
#         logger.info('Spark Session is stopped')
        





#  Full Outer join

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from utils.logger import logger

# spark=None


# try:
#     logger.info("Starting Spark Session")
#     spark=(
#         SparkSession.builder
#         .master('local[2]')
#         .appName('Right Join')
#         .getOrCreate()
#     )
    
#     logger.info('Spark Session Created')
    
#     logger.info('Reading customers.csv dataframe or file')
#     customers=spark.read.csv(
#         'data/joins/customers.csv',
#         header=True,
#         inferSchema=True
#     )
    
#     logger.info('Reading orders.csv dataframe or file')
#     orders=spark.read.csv(
#         'data/joins/orders.csv',
#         header=True,
#         inferSchema=True
#     )
    
#     logger.info('Creating alias for customers and orders dataframe')
#     c=customers.alias('c')
#     o=orders.alias('o')
    
#     logger.info('Performing Full Outer join')
    
#     full_join_df=(
#         c.join(
#             o,
#             col('c.customer_id')==col('o.customer_id'),
#             'full'
#         )
#         .select(
#             col('c.customer_id'),
#             col("c.customer_name"),
#             col("c.city"),
#             col("o.order_id"),
#             col("o.amount")
#         )
#     )
    
#     logger.info('Results after performing right join')
#     full_join_df.show()
    
#     logger.info('Customers with no orders')
#     customers_without_order=full_join_df.filter(
#         col('order_id').isNull()
#     )
#     customers_without_order.show()
    
#     logger.info('Orders without customers')
#     orders_without_customers=(
#         full_join_df.filter(
#             col('customer_id').isNull()
#         )
#     )
    
#     orders_without_customers.show()
    
    
#     logger.info('count both categories')
#     customers_without_orders_count = customers_without_order.count()
#     orders_without_customers_count = orders_without_customers.count()
    
#     logger.info(f'Customer with no orders: {customers_without_orders_count}')
#     logger.info(f'Orders with no customer: {orders_without_customers_count}')
    
    
    
    
# except Exception as e:
#     logger.info(f'Application Failed: {e}')

# finally:
#     if spark:
#         input('Press Enter to stop')
#         spark.stop()
#         logger.info('Spark Session is stopped')
  







# Left semi join



# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from utils.logger import logger

# spark=None


# try:
#     logger.info("Starting Spark Session")
#     spark=(
#         SparkSession.builder
#         .master('local[2]')
#         .appName('Right Join')
#         .getOrCreate()
#     )
    
#     logger.info('Spark Session Created')
    
#     logger.info('Reading customers.csv dataframe or file')
#     customers=spark.read.csv(
#         'data/joins/customers.csv',
#         header=True,
#         inferSchema=True
#     )
    
#     logger.info('Reading orders.csv dataframe or file')
#     orders=spark.read.csv(
#         'data/joins/orders.csv',
#         header=True,
#         inferSchema=True
#     )
    
#     logger.info('Creating alias for customers and orders dataframe')
#     c=customers.alias('c')
#     o=orders.alias('o')
    
#     logger.info('Performing Left Semi join')
    
#     left_semi_join_df=(
#         c.join(
#             o,
#             col('c.customer_id')==col('o.customer_id'),
#             'left_semi'
#         )
#     )
    
#     left_semi_join_df.show()
    
#     logger.info('Counting the customer who placed at least one order ')
#     count=left_semi_join_df.count()
#     logger.info(f'checking the count of customers who placed at least one order: {count}')
    
    
# except Exception as e:
#     logger.info(f'Application Failed: {e}')

# finally:
#     if spark:
#         input('Press Enter to stop')
#         spark.stop()
#         logger.info('Spark Session is stopped')
  
  
  


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.logger import logger


spark=None

try:
    logger.info('Creating Spark Session')
    spark=(
        SparkSession.builder 
        .master('local[2]')
        .appName('Left Anti join')
        
        .getOrCreate()
    )
    logger.info('Spark Session Created')
    
    logger.info('Reading Customers.csv dataframe or file')
    
    customers=spark.read.csv(
        'data/joins/customers.csv',
        header=True,
        inferSchema=True
    )
    
    logger.info('Reading Order.csv dataframe or file')
    orders=spark.read.csv(
        'data/joins/orders.csv',
        header=True,
        inferSchema=True
        
    )
    
    logger.info('creating alias for customers and orders dataframe')
    
    c=customers.alias('c')
    o=orders.alias('o')
    
    logger.info('Performing Left Anti join')
    
    left_anti_join_df=(
        c.join(
            o,
            col('c.customer_id')==col('o.customer_id'),
            'left_anti'
        )
    )
    
    left_anti_join_df.show()
    
    customer_count=left_anti_join_df.count()
    logger.info(f'Customer who never placed an order: {customer_count}')
    
    logger.info('Printing Schema')
    left_anti_join_df.printSchema()

except Exception as e:
    logger.info(f'Application is Failed {e}')

finally:
    if spark:
        input('Press Enter to stop')
        spark.stop()
        logger.info("Spark Session is stopped")
