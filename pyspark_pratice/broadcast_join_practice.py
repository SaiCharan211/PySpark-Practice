from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from utils.logger import logger

spark = None

try:
    logger.info("Creating Spark Session")

    spark = (
        SparkSession.builder
        .master("local[4]")
        .appName("Module 10.1 - Broadcast Join")
        .getOrCreate()
    )

    logger.info("Spark Session Created Successfully")


    logger.info("Reading employees.csv")

    employees = (
        spark.read
        .csv(
            "data/employees/employees.csv",
            header=True,
            inferSchema=True
        )
    )

    logger.info("Reading departments.csv")

    departments = (
        spark.read
        .csv(
            "data/employees/departments.csv",
            header=True,
            inferSchema=True
        )
    )

    logger.info("Employees DataFrame")

    employees.show()

    logger.info("Departments DataFrame")

    departments.show()

    logger.info("Employees Schema")

    employees.printSchema()

    logger.info("Departments Schema")

    departments.printSchema()

    logger.info(f"Employee Count : {employees.count()}")

    logger.info(f"Department Count : {departments.count()}")


    logger.info("Creating DataFrame Aliases")

    e = employees.alias("e")
    d = departments.alias("d")

    logger.info("Performing Normal Inner Join")

    normal_join_df = (
        e.join(
            d,
            col("e.department_id") == col("d.department_id"),
            "inner"
        )
        .select(
            col("e.employee_id"),
            col("e.name"),
            col("d.department_name"),
            col("e.salary")
        )
    )

    logger.info("Normal Join Result")

    normal_join_df.show()

    logger.info("Execution Plan (Shuffle Join)")

    normal_join_df.explain()

    logger.info("Performing Broadcast Join")

    broadcast_join_df = (
        e.join(
            broadcast(d),
            col("e.department_id") == col("d.department_id"),
            "inner"
        )
        .select(
            col("e.employee_id"),
            col("e.name"),
            col("d.department_name"),
            col("e.salary")
        )
    )

    logger.info("Broadcast Join Result")

    broadcast_join_df.show()

    logger.info("Execution Plan (Broadcast Join)")

    broadcast_join_df.explain()

    logger.info("Filtering Employees (salary > 60000)")

    filtered_employees = (
        e.filter(
            col("salary") > 60000
        )
    )

    logger.info("Joining Filtered Employees with Departments")

    optimized_join_df = (
        filtered_employees.join(
            broadcast(d),
            filtered_employees.department_id == d.department_id,
            "inner"
        )
        .select(
            filtered_employees.employee_id,
            filtered_employees.name,
            d.department_name,
            filtered_employees.salary
        )
    )

    logger.info("Optimized Join Result")

    optimized_join_df.show()

    logger.info("Optimized Join Execution Plan")

    optimized_join_df.explain()

  
    logger.info("Sorting Final Data by Salary Descending")

    final_df = (
        optimized_join_df.orderBy(
            col("salary").desc()
        )
    )

    logger.info("Final DataFrame")

    final_df.show()

    logger.info("Final Schema")

    final_df.printSchema()

    logger.info(f"Final Record Count : {final_df.count()}")

except Exception:

    logger.exception("Application Failed")

finally:

    if spark:

        input("Press Enter to Stop Spark Session...")

        spark.stop()

        logger.info("Spark Session Stopped Successfully")