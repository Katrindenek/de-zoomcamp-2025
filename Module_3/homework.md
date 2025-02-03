## Module 3 Data Warehouse Homework

For this homework we will be using the Yellow Taxi Trip Records for **January 2024 - June 2024 NOT the entire year of data** 
Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>

**Load Script:** You can manually download the parquet files and upload them to your GCS Bucket or you can use the linked script [here](./load_yellow_taxi_data.py):<br>
You will simply need to generate a Service Account with GCS Admin Priveleges or be authenticated with the Google SDK and update the bucket name in the script to the name of your bucket<br>
Nothing is fool proof so make sure that all 6 files show in your GCS Bucket before begining.</br><br>

<b>BIG QUERY SETUP:</b></br>
Create an external table using the Yellow Taxi Trip Records. </br>
Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table). </br>
</p>

```sql
-- create external table
CREATE EXTERNAL TABLE `mystic-primacy-447818-n5.ny_taxi_2024.my_taxi_2024`
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://dezoomcamp_hw3_2025_katrine/*.parquet']
    );

-- create materialised table
CREATE OR REPLACE TABLE `mystic-primacy-447818-n5.ny_taxi_2024.my_taxi_2024_materialised`
AS SELECT * FROM `mystic-primacy-447818-n5.ny_taxi_2024.my_taxi_2024`;
```

## Question 1:
Question 1: What is count of records for the 2024 Yellow Taxi Data?
- 65,623
- 840,402
- 20,332,093
- 85,431,289

### The answer: 20,332,093

### Explanation: 

```sql
SELECT COUNT(*) 
FROM `mystic-primacy-447818-n5.ny_taxi_2024.my_taxi_2024`;
```

---


## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 155.12 MB for the Materialized Table
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

### The answer: 0 MB for the External Table and 155.12 MB for the Materialized Table

### Explanation:

The estimated amount of data is shown at the top right of the screen and says "This script will process * MB when run."

```sql
SELECT COUNT(DISTINCT PULocationID)
FROM `mystic-primacy-447818-n5.ny_taxi_2024.my_taxi_2024`;
-- 0B

SELECT COUNT(DISTINCT PULocationID)
FROM `mystic-primacy-447818-n5.ny_taxi_2024.my_taxi_2024_materialised`;
-- 155.12 MB
```

---

## Question 3:
Write a query to retrieve the PULocationID form the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, 
doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

### The answer: 

BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

---

## Question 4:
How many records have a fare_amount of 0?
- 128,210
- 546,578
- 20,188,016
- 8,333

### The answer: 8,333

### Explanation: 

```sql
SELECT COUNTIF(fare_amount = 0)
FROM `mystic-primacy-447818-n5.ny_taxi_2024.my_taxi_2024_materialised`;
```

---

## Question 5:
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_timedate and order the results by VendorID (Create a new table with this strategy)
- Partition by tpep_dropoff_timedate and Cluster on VendorID
- Cluster on by tpep_dropoff_timedate and Cluster on VendorID
- Cluster on tpep_dropoff_timedate Partition by VendorID
- Partition by tpep_dropoff_timedate and Partition by VendorID

### The answer: Partition by tpep_dropoff_timedate and Cluster on VendorID

### Explanation:

**Partitioning**: Partitioning the table by `tpep_dropoff_datetime` will optimize the query performance when filtering by this column. Partitioning helps in reducing the amount of data scanned by narrowing down the data to specific partitions.

**Clustering**: Clustering the table by `VendorID` will optimize the query performance when ordering by this column. Clustering helps in organizing the data within each partition, making it more efficient to retrieve and sort the data.

```sql
CREATE TABLE `mystic-primacy-447818-n5.ny_taxi_2024.my_taxi_2024_optimised`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `mystic-primacy-447818-n5.ny_taxi_2024.my_taxi_2024_materialised`;
```

## Question 6:
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_timedate
03/01/2024 and 03/15/2024 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

### The answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

### Explanation:

The partition and clustering decreased the estimated processed bytes as expected.

---

## Question 7: 
Where is the data stored in the External Table you created?

- Big Query
- Container Registry
- GCP Bucket
- Big Table

### The answer: GCP Bucket

### Explanation:

When you create an external table in BigQuery, the data is not stored in BigQuery itself. Instead, it remains in its original location, such as a Google Cloud Storage (GCP) bucket. BigQuery accesses the data directly from this external source when you run queries.

---

## Question 8:
It is best practice in Big Query to always cluster your data:
- True
- False

### The answer: False

### Explanation:

Clustering should be used when you have specific query patterns that can benefit from it, such as frequent filtering or sorting on clustered columns. However, for some datasets or query patterns, clustering may not provide a noticeable performance improvement and could add unnecessary complexity.


## (Bonus: Not worth points) Question 8:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

### The answer: 0 B

### Explanation: 

The estimated bytes is 0, because the query result is cached by BigQuery.


