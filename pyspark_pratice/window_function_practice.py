from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,
    row_number,
    rank, 
    dense_rank,
    lag,
    lead,
    avg,
    sum,
    count,
    max,
    min)
from pyspark.sql.window import Window
from utils.logger import logger

spark = None

try:

    logger.info("Creating Spark Session")

    spark = (
        SparkSession.builder
        .master("local[3]")
        .appName("Module 11 - Window Specification")
        .getOrCreate()
    )

    logger.info("Reading employees_window.csv")

    employees = (
        spark.read.csv(
            "data/employees.csv",
            header=True,
            inferSchema=True
        )
    )

    logger.info("Original Data")

    employees.show()
    
    logger.info('Creating Window Specification')
    
    salary_window=(
        Window
        .partitionBy('department')
        .orderBy(col('salary').desc())
    )
    
    
    
    department_window=(
        Window
        .partitionBy('department')
    )
    
    
    
    logger.info('Row number')
    q1=(
        employees.withColumn(
            'row_number',
            row_number().over(salary_window)
        )
    )
    q1.show()

    logger.info('Rank')
    
    q2=(
        employees.withColumn(
            'rank',
            rank().over(salary_window)
        )
    )
    q2.show()
    
    logger.info('Dense Rank')
    
    q3=(
        employees.withColumn(
            'dense_rank',
            dense_rank().over(salary_window)
        )
    )
    q3.show()
    
    logger.info('Highest Paid Employees')
    
    q4 = (
        employees
        .withColumn(
            "rank",
            rank()
            .over(salary_window)
        )
        .filter(
            col("rank") == 1
        )
        .drop("rank")
    )

    q4.show()
    
    
    logger.info('Second Highest Salary')
    q5 = (
        employees
        .withColumn(
            "dense_rank",
            dense_rank()
            .over(salary_window)
        )
        .filter(
            col("dense_rank") == 2
        )
        .drop("dense_rank")
    )

    q5.show()
    
    
    #  Value Functions
    
    logger.info('previous Salary')
    
    q6=(
        employees.withColumn(
            'previous_salary',
            lag('salary',1).over(salary_window)
        )
    )
    q6.show()
    
    logger.info('Next Salary')
    
    q7=(
        employees.withColumn(
            'next_salary',
            lead('salary',1).over(salary_window)
        )
    )
    
    q7.show()
    
    logger.info('Highest Salary')
    q8=(
        employees.withColumn(
            'highest_salary',
            max(col('salary')).over(department_window)
        )
    )
    q8.show()
    
    logger.info('Lowest Salary')
    
    q9=(
        employees.withColumn(
            'lowest_salary',
            min(col('salary')).over(department_window)
        )
    )
    q9.show()
    
    
    # Aggregate Window Function
    
    logger.info('Department Avg Salary')
    
    q10=(
        employees.withColumn(
            'department_avg_salary',
            avg(col('salary')).over(department_window)
        )
    )
    q10.show()
    
    logger.info('Department Total Salary')
    
    q11=(
        employees.withColumn(
            'department_total_salary',
            sum(col('salary')).over(department_window)
        )
    )
    
    q11.show()
    
    
    logger.info('Department Employee Count')
    
    q12=(
        employees.withColumn(
            'department_employee_count',
            count('*').over(department_window)
        )
    )
    q12.show()
    
    logger.info('Department Maximum salary')
    
    q13=(
        employees.withColumn(
            'department_max_salary',
            max(col('salary')).over(department_window)
        )
    )
    q13.show()
    
    logger.info('Department Minimum Salary')
    
    q14=(
        employees.withColumn(
            'department_min_salary',
            min(col('salary')).over(department_window)
        )
    )
    q14.show()
    
    
    # Running Total Salary
    
    logger.info('Running Total Salary')
    
    q15 = (
        employees
        .withColumn(
            "running_total_salary",
            sum("salary")
            .over(
                Window
                .orderBy("joining_date")
                .rowsBetween(
                    Window.unboundedPreceding,
                    Window.currentRow
                )
            )
        )
    )

    q15.show()
    
    
    logger.info('Running Employee Count')
    q16=(
        employees.withColumn(
            'employee_running_count',
            count('*').over(
                Window
                .orderBy('joining_date')
                .rowsBetween(
                    Window.unboundedPreceding,
                    Window.currentRow
                )
            )
        )
    )
    
    q16.show()
    
    
    logger.info('Moving Avg salary')
    
    q17=(
        employees.withColumn(
            'moving_avg_salary',
            avg(col('salary')).over(
                Window
                .orderBy('joining_date')
                .rowsBetween(
                   -1,1
                )
            )
        )
    )
    
    q17.show()
    
    
    logger.info('Latest Employee In Department')
    
    q18=(
        employees.withColumn(
            'row_number',
            row_number().over(
                Window 
                .partitionBy('department')
                .orderBy(col('joining_date').desc())
            )
        ).filter(col('row_number')==1)
    )
    
    q18.show()
    
    logger.info('Top 3 Highest Paid Employee')
    
    q19=(
        employees.withColumn(
            'rank',
            row_number().over(
                Window
                .partitionBy('department')
                .orderBy(col('salary').desc())
            )
        ).filter(col('rank')<=3)
    )
    
    q19.show()
    
    logger.info('Salary Greater Than Department Average')
    
    q20=(
        employees.withColumn(
            'department_avg_salary',
            avg('salary').over(
                Window
                .partitionBy('department')
            )
        ).filter(
            col('salary')>col('department_avg_salary')
        )    
    )
    
    q20.show()
except Exception:

    logger.exception("Application Failed")

finally:

    if spark:

        input("Press Enter to Stop Spark Session")

        spark.stop()

        logger.info("Spark Session Stopped")


