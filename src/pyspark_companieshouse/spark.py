"""Example PySpark client that talks to a remote Spark Connect server.

Setup:
    pip install "pyspark[connect]==3.5.3"
    docker compose up -d
    python spark.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# from pyspark.sql.functions import col

SPARK_REMOTE = "sc://localhost:15002"


def main():
    spark = SparkSession.builder.remote(SPARK_REMOTE).appName("example").getOrCreate()
    print(f"Connected to Spark {spark.version}")
    df = spark.read.csv('/data/BasicCompanyDataAsOneFile-2026-10-01.csv', header=True, inferSchema=True)
    print(df.show())
    # total number of companies
    print(f"Total number of companies: {df.count()}")

    # top 50 companies by number of outstanding mortgages
    print(df.filter(df.CompanyStatus == "Active").orderBy(F.col("Mortgages.NumMortOutstanding")).limit(50).show())


    spark.stop()


if __name__ == "__main__":
    main()
