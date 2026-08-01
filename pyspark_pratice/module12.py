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
        .appName("Module 12 - Handling Null Values")
        .getOrCreate()
    )

    logger.info("Spark Session Created Successfully")

    # --------------------------------------------------
    # Read CSV File
    # --------------------------------------------------

    logger.info("Reading employees_null.csv")

    employees = spark.read.csv(
        "data/employees_null.csv",
        header=True,
        inferSchema=True
    )

    # --------------------------------------------------
    # Original Data
    # --------------------------------------------------

    logger.info("Original DataFrame")

    employees.show()

    logger.info("Original Schema")

    employees.printSchema()

    original_count = employees.count()

    logger.info(f"Original Row Count : {original_count}")

    # --------------------------------------------------
    # Find NULL Salary
    # --------------------------------------------------

    logger.info("Employees with NULL Salary")

    employees.filter(
        col("salary").isNull()
    ).show()

    # --------------------------------------------------
    # Find NULL Name
    # --------------------------------------------------

    logger.info("Employees with NULL Name")

    employees.filter(
        col("name").isNull()
    ).show()

    # --------------------------------------------------
    # Find Non-NULL Email
    # --------------------------------------------------

    logger.info("Employees with Non-NULL Email")

    employees.filter(
        col("email").isNotNull()
    ).show()

    # --------------------------------------------------
    # Replace NULL Values
    # --------------------------------------------------

    logger.info("Replacing NULL Department and Salary")

    cleaned_df = (
        employees
        .fillna(
            {
                "department": "Unknown",
                "salary": 0
            }
        )
    )

    # --------------------------------------------------
    # Replace Department Names
    # --------------------------------------------------

    logger.info("Replacing HR with Human Resources")

    cleaned_df = cleaned_df.replace(
        "HR",
        "Human Resources",
        subset=["department"]
    )

    # --------------------------------------------------
    # Drop Invalid Records
    # --------------------------------------------------

    logger.info("Dropping Rows Where employee_id is NULL")

    cleaned_df = cleaned_df.dropna(
        subset=["employee_id"]
    )

    # --------------------------------------------------
    # Final Data
    # --------------------------------------------------

    logger.info("Final Cleaned DataFrame")

    cleaned_df.show()

    logger.info("Final Schema")

    cleaned_df.printSchema()

    final_count = cleaned_df.count()

    logger.info(f"Final Row Count : {final_count}")

except Exception:

    logger.exception("Application Failed")

finally:

    if spark:

        input("Press Enter to Stop Spark Session...")

        spark.stop()

        logger.info("Spark Session Stopped Successfully")