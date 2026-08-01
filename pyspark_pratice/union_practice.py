from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.logger import logger

spark = None

try:
    # --------------------------------------------------
    # Create Spark Session
    # --------------------------------------------------

    logger.info("Creating Spark Session")

    spark = (
        SparkSession.builder
        .master("local[3]")
        .appName("Module 11 - Union Operations")
        .getOrCreate()
    )

    logger.info("Spark Session Created Successfully")

    # --------------------------------------------------
    # Read CSV Files
    # --------------------------------------------------

    logger.info("Reading employees_day1.csv")

    employees_day1 = spark.read.csv(
        "data/unions/employees_day1.csv",
        header=True,
        inferSchema=True
    )

    logger.info("Reading employees_day2.csv")

    employees_day2 = spark.read.csv(
        "data/unions/employees_day2.csv",
        header=True,
        inferSchema=True
    )

    logger.info("Reading employees_day3.csv")

    employees_day3 = spark.read.csv(
        "data/unions/employees_day3.csv",
        header=True,
        inferSchema=True
    )

    # --------------------------------------------------
    # Show Input Data
    # --------------------------------------------------

    logger.info("Displaying Day 1 Data")

    employees_day1.show()

    logger.info("Displaying Day 2 Data")

    employees_day2.show()

    logger.info("Displaying Day 3 Data")

    employees_day3.show()

    # --------------------------------------------------
    # Print Schema
    # --------------------------------------------------

    logger.info("Day 1 Schema")

    employees_day1.printSchema()

    logger.info("Day 2 Schema")

    employees_day2.printSchema()

    logger.info("Day 3 Schema")

    employees_day3.printSchema()

    # --------------------------------------------------
    # Count Records
    # --------------------------------------------------

    day1_count = employees_day1.count()
    day2_count = employees_day2.count()
    day3_count = employees_day3.count()

    logger.info(f"Day 1 Rows : {day1_count}")
    logger.info(f"Day 2 Rows : {day2_count}")
    logger.info(f"Day 3 Rows : {day3_count}")

    # --------------------------------------------------
    # Union Day1 + Day2
    # --------------------------------------------------

    logger.info("Combining Day 1 and Day 2 using union()")

    combined_df = employees_day1.union(employees_day2)

    logger.info("Day 1 and Day 2 Combined Successfully")

    # --------------------------------------------------
    # Union Day3 using unionByName()
    # --------------------------------------------------

    logger.info("Adding Day 3 using unionByName()")

    final_df = combined_df.unionByName(
        employees_day3,
        allowMissingColumns=True
    )

    logger.info("Day 3 Added Successfully")

    # --------------------------------------------------
    # Sort Data
    # --------------------------------------------------

    logger.info("Sorting Employees by Salary Descending")

    final_df = final_df.orderBy(
        col("salary").desc()
    )

    # --------------------------------------------------
    # Final Output
    # --------------------------------------------------

    logger.info("Displaying Final DataFrame")

    final_df.show()

    logger.info("Final Schema")

    final_df.printSchema()

    final_count = final_df.count()

    logger.info(f"Total Records : {final_count}")
    
    
    final_df.columns
    final_df.describe().show()

except Exception:

    logger.exception("Application Failed")

finally:

    if spark:

        input("Press Enter to Stop Spark Session...")

        spark.stop()

        logger.info("Spark Session Stopped Successfully")