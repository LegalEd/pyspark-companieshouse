"""Example PySpark client that talks to a remote Spark Connect server.

Setup:
    pip install "pyspark[connect]==3.5.3"
    docker compose up -d
    python spark.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StructType, StructField, StringType


SPARK_REMOTE = "sc://localhost:15002"

# schema = StructType([
#     StructField("CompanyName", StringType(), True),
#     StructField("CompanyNumber", StringType(), True),
#     StructField("Mortgages.NumMortCharges", IntegerType(), True),
#     StructField("Mortgages.NumMortOutstanding", IntegerType(), True),
#     StructField("Mortgages.NumMortPartSatisfied", IntegerType(), True),
#     StructField("Mortgages.NumMortSatisfied", IntegerType(), True),

# ])


def main():
    spark = SparkSession.builder.remote(SPARK_REMOTE).appName("example").getOrCreate()
    print(f"Connected to Spark {spark.version}")
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("/data/BasicCompanyDataAsOneFile-2026-10-01.csv")
        .withColumn(
            "Mortgages.NumMortOutstanding",
            F.col("`Mortgages.NumMortOutstanding`").cast("int"),
        )
    )
    # print(df.show())
    # total number of companies
    print(f"Total number of companies: {df.count()}")

    # top 50 companies by number of outstanding mortgages
    df_top_50 = (
        df.filter(df.CompanyStatus == "Active")
        .orderBy(F.col("`Mortgages.NumMortOutstanding`").desc())
        .limit(50)
    )
    print(df_top_50.select("CompanyName", "`Mortgages.NumMortOutstanding`").show())

    spark.stop()


if __name__ == "__main__":
    main()
