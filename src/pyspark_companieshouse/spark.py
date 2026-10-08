"""Example PySpark client that talks to a remote Spark Connect server.

Setup:
    pip install "pyspark[connect]>=4.2.0"
    docker compose up -d
    python spark.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
import pyspark.sql.functions as sf


SPARK_REMOTE = "sc://localhost:15002"

def main():
    spark = SparkSession.builder.remote(SPARK_REMOTE).appName("example").getOrCreate()
    print(f"Connected to Spark {spark.version}")
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv("/data/BasicCompanyDataAsOneFile-2026-10-01.csv")
        .withColumn(
            "Mortgages.NumMortOutstanding",
            F.col("`Mortgages.NumMortOutstanding`").try_cast("int"),
        )
        .withColumn("IncorporationDate", F.to_date("IncorporationDate", "dd/MM/yyyy"))
        .withColumn("DissolutionDate", F.to_date("DissolutionDate", "dd/MM/yyyy"))
        .withColumn("EndDate", F.to_date(F.lit("2026-10-30")))
        .withColumn("CountryOfOrigin", F.upper("CountryOfOrigin"))
    )
    print(f"Total number of companies: {df.count()}")

    print("Top 50 active companies with the most outstanding mortgages")
    df_top_50 = (
        df.filter(df.CompanyStatus == "Active")
        .orderBy(F.col("`Mortgages.NumMortOutstanding`").desc())
        .limit(50)
    )
    print(
        df_top_50.select("CompanyName", "`Mortgages.NumMortOutstanding`").show(
            n=50, truncate=False
        )
    )

    df_oldest = (
        df.filter(df.CompanyStatus == "Active")
        .orderBy(F.col("IncorporationDate"))
        .limit(1)
    )

    print("The oldest active company is...")
    print(
        df_oldest.select("CompanyName", "IncorporationDate").show(n=1, truncate=False)
    )

    print("The top 10 must commonly used addresses....")

    df_addresses = (
        (
            df.filter(
                (df.CompanyStatus == "Active")
                & (df["`RegAddress.AddressLine1`"].isNotNull())
            ).groupBy(
                [
                    "`RegAddress.AddressLine1`",
                    "`RegAddress.PostTown`",
                    "`RegAddress.PostCode`",
                ]
            )
        )
        .count()
        .orderBy("count", ascending=False)
    )

    print(df_addresses.show(n=10, truncate=False))


    print("The average tenure (days) of a Active company is...")
    df_tenure = (df.filter(df.CompanyStatus == "Active"))
    df_tenure = df_tenure.select("*", sf.datediff("EndDate", "IncorporationDate").alias('Tenure'))
    print(df_tenure.select(sf.avg("Tenure").alias("AverageTenure")).show(n=1))

    print("The first limited partnership was:")

    df_lp = (df.filter(df.CompanyCategory == "Limited Partnership").orderBy("IncorporationDate").limit(1))
    print(df_lp.select(["CompanyName", "IncorporationDate"]).show())


    print("The distribution of companies by country of origin is:")
    print(df.groupBy("CountryOfOrigin").count().orderBy("count",ascending=False).show(n=300, truncate=False))

    spark.stop()


if __name__ == "__main__":
    main()
