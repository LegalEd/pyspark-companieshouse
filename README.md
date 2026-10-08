# PySpark Company Data Analysis

An example **PySpark** client that connects to a remote **Spark Connect** server and performs analysis on UK company data.

The script reads company information from a CSV file, transforms several fields, and produces a number of statistics and reports, including:

- The total number of companies
- The 50 active companies with the most outstanding mortgages
- The oldest active company
- The most commonly used registered addresses
- The average tenure of active companies
- The first registered Limited Partnership
- The distribution of companies by country of origin

## Requirements

- Python 3.10+
- Docker and Docker Compose
- PySpark with Spark Connect support
- A running Spark Connect server

Install the required Python dependency with:

```bash
pip install "pyspark[connect]>=4.2.0"
```

## Project Structure

A minimal project setup looks like this:

```text
.
├── spark.py
├── README.md
└── data/
    └── BasicCompanyDataAsOneFile-2026-10-01.csv
```

The CSV file must be available to the Spark environment at:

```text
/data/BasicCompanyDataAsOneFile-2026-10-01.csv
```

> **Important:** `/data/...` is resolved by the Spark environment, not necessarily your local Python process. If Spark is running inside Docker, make sure the local `data` directory is mounted into the container as `/data`.

## Running the Application

### 1. Start the Spark Connect server

Start the required Docker services:

```bash
docker compose up -d
```

Verify that the Spark Connect server is running and listening on:

```text
sc://localhost:15002
```

### 2. Install PySpark

```bash
pip install "pyspark[connect]>=4.2.0"
```

### 3. Run the script

```bash
python spark.py
```

The script will connect to the remote Spark server and print the results of each analysis to the console.

## Spark Connect

The script uses Spark Connect rather than creating a local Spark session:

```python
SPARK_REMOTE = "sc://localhost:15002"

spark = (
    SparkSession.builder
    .remote(SPARK_REMOTE)
    .appName("example")
    .getOrCreate()
)
```

This means the Python application acts as a client while Spark executes the data processing remotely.

The connection endpoint can be changed by modifying:

```python
SPARK_REMOTE = "sc://localhost:15002"
```

## Data Processing

After loading the CSV file, the script performs several transformations.

### Mortgage Count

The `Mortgages.NumMortOutstanding` column is explicitly converted to an integer:

```python
.withColumn(
    "Mortgages.NumMortOutstanding",
    F.col("`Mortgages.NumMortOutstanding`").try_cast("int"),
)
```

`try_cast` allows invalid values to become `NULL` rather than causing the entire operation to fail.

### Dates

The incorporation and dissolution dates are converted from the source format:

```text
dd/MM/yyyy
```

into Spark date values.

```python
.withColumn(
    "IncorporationDate",
    F.to_date("IncorporationDate", "dd/MM/yyyy")
)
.withColumn(
    "DissolutionDate",
    F.to_date("DissolutionDate", "dd/MM/yyyy")
)
```

### End Date

An analysis end date of **30 October 2026** is added:

```python
.withColumn("EndDate", F.to_date(F.lit("2026-10-30")))
```

This is used when calculating company tenure.

### Country of Origin

Country names are converted to uppercase:

```python
.withColumn("CountryOfOrigin", F.upper("CountryOfOrigin"))
```

This helps normalise the country values before grouping them.

---

# Analysis

## Total Number of Companies

The script calculates the total number of records:

```python
df.count()
```

Output resembles:

```text
Total number of companies: <number>
```

---

## Top 50 Active Companies by Outstanding Mortgages

Only companies with an `Active` status are considered.

The companies are sorted by the number of outstanding mortgages in descending order and limited to 50 results.

```python
df.filter(df.CompanyStatus == "Active")
  .orderBy(
      F.col("`Mortgages.NumMortOutstanding`").desc()
  )
  .limit(50)
```

The following fields are displayed:

- Company name
- Number of outstanding mortgages

---

## Oldest Active Company

The script identifies the active company with the earliest incorporation date:

```python
df.filter(df.CompanyStatus == "Active")
  .orderBy(F.col("IncorporationDate"))
  .limit(1)
```

The output contains:

- Company name
- Incorporation date

---

## Most Common Registered Addresses

The script identifies the 10 most frequently used registered addresses among active companies.

Companies without an `AddressLine1` are excluded.

The grouping uses:

- Address Line 1
- Post Town
- Postcode

The results are then ordered by the number of companies associated with each address.

```python
.groupBy(
    [
        "`RegAddress.AddressLine1`",
        "`RegAddress.PostTown`",
        "`RegAddress.PostCode`",
    ]
)
.count()
.orderBy("count", ascending=False)
```

---

## Average Active Company Tenure

The script calculates the number of days between the incorporation date and the analysis end date:

```python
sf.datediff(
    "EndDate",
    "IncorporationDate"
)
```

This value is stored as `Tenure`.

The average tenure is then calculated across active companies:

```python
sf.avg("Tenure")
```

The result represents the **average number of days an active company has existed as of 30 October 2026**.

> Note: This calculation uses the fixed `EndDate` rather than the current date.

---

## First Limited Partnership

The script finds the earliest incorporated company whose category is:

```text
Limited Partnership
```

The records are ordered by incorporation date and the first result is returned.

The output contains:

- Company name
- Incorporation date

---

## Company Distribution by Country of Origin

Finally, the script groups companies by `CountryOfOrigin` and counts the number of companies in each group.

The results are ordered from the most common country to the least common:

```python
df.groupBy("CountryOfOrigin") \
  .count() \
  .orderBy("count", ascending=False)
```

Up to 300 results are displayed.

---

# Configuration

The following values are currently hard-coded in the script:

| Setting                | Value                                            |
| ---------------------- | ------------------------------------------------ |
| Spark Connect server   | `sc://localhost:15002`                           |
| Input CSV              | `/data/BasicCompanyDataAsOneFile-2026-10-01.csv` |
| Analysis end date      | `2026-10-30`                                     |
| Top mortgage companies | `50`                                             |
| Top addresses          | `10`                                             |
| Country results        | `300`                                            |

For a production application, these values could be moved into environment variables or command-line arguments.

## Example

For example, the Spark endpoint could be configured using an environment variable:

```python
import os

SPARK_REMOTE = os.getenv(
    "SPARK_REMOTE",
    "sc://localhost:15002"
)
```

This would allow the application to be run against different Spark environments without modifying the source code.

---

# Stopping the Spark Environment

When finished, the Docker services can be stopped with:

```bash
docker compose down
```

The Python script also explicitly closes the Spark session:

```python
spark.stop()
```

---

# Troubleshooting

### Cannot connect to Spark Connect

If the script fails to connect to:

```text
sc://localhost:15002
```

check that the Docker services are running:

```bash
docker compose ps
```

You can also inspect the container logs:

```bash
docker compose logs
```

### CSV file cannot be found

If Spark reports that the CSV file does not exist, verify that the file is available inside the Spark environment at:

```text
/data/BasicCompanyDataAsOneFile-2026-10-01.csv
```

If using Docker, check the volume mapping in `docker-compose.yml`.

### Incorrect date values

The script expects dates in:

```text
dd/MM/yyyy
```

format.

For example:

```text
01/10/2026
```

If the source data uses another format, the `F.to_date()` format strings will need to be updated.

### Null mortgage values

Invalid or missing mortgage values may become `NULL` because the script uses `try_cast("int")`.

This is intentional and prevents malformed values from causing the transformation to fail.

---

# Notes

This project is intended as an example of using **PySpark with Spark Connect** to perform distributed data processing from a Python client.

The script demonstrates several common Spark operations:

- Reading CSV data
- Schema inference
- Type conversion
- Date manipulation
- Filtering
- Sorting
- Grouping
- Aggregation
- Limiting results
- Using Spark SQL functions
- Connecting to a remote Spark session

It is primarily intended for demonstration and development rather than production use.