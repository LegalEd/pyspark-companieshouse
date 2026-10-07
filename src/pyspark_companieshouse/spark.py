"""Example PySpark client that talks to a remote Spark Connect server.

Setup:
    pip install "pyspark[connect]==3.5.3"
    docker compose up -d
    python spark.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SPARK_REMOTE = "sc://localhost:15002"


def main():
    spark = SparkSession.builder.remote(SPARK_REMOTE).appName("example").getOrCreate()
    print(f"Connected to Spark {spark.version}")

    # Example data
    data = [
        ("Alice", "Engineering", 120000),
        ("Bob", "Engineering", 105000),
        ("Carla", "Sales", 80000),
        ("Dmitri", "Sales", 95000),
        ("Eve", "Marketing", 70000),
        ("Frank", "Marketing", 72000),
    ]
    df = spark.createDataFrame(data, schema=["name", "department", "salary"])

    print("Original data:")
    df.show()

    # Transformations are lazy and executed on the server
    summary = (
        df.filter(F.col("salary") > 71000)
        .groupBy("department")
        .agg(
            F.count("*").alias("employees"),
            F.round(F.avg("salary"), 2).alias("avg_salary"),
            F.max("salary").alias("max_salary"),
        )
        .orderBy(F.desc("avg_salary"))
    )

    print("Summary by department (salary > 71000):")
    summary.show()

    # Bring a (small) result back to the client as a list of Rows
    for row in summary.collect():
        print(row.department, row.avg_salary)

    spark.stop()


if __name__ == "__main__":
    main()
