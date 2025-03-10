# Module 5 Batch processing Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2024-10 data from the official website: 

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
```


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

### The answer: 3.3.2

## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB
- 75MB
- 100MB

### The answer: 25 MB

### Code:

```python
!wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet

df = spark.read \
    .option("header", "true") \
    .parquet('yellow_tripdata_2024-10.parquet') \
    .repartition(4)

df.write.parquet('yellow/2024/10/')
```

## Question 3: Count records 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 85,567
- 105,567
- 125,567
- 145,567

### The answer: 125,567 is the closest option

### Code:

```python
df_yellow = spark.read.parquet('yellow/*/*')

df_yellow.createOrReplaceTempView('trips_data')

spark.sql("""
SELECT
    count(1)
FROM
    trips_data
WHERE
    CAST(tpep_pickup_datetime AS DATE) = '2024-10-15'
    AND trip_distance > 0
""").show()
```

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 122
- 142
- 162
- 182

### The answer: 162

### Code:

```python
df_yellow = spark.read.parquet('yellow/*/*')

df_yellow.createOrReplaceTempView('trips_data')

spark.sql("""
SELECT
    timestampdiff(HOUR, tpep_pickup_datetime, tpep_dropoff_datetime)
FROM
    trips_data
WHERE
    trip_distance > 0
ORDER BY
    timestampdiff(HOUR, tpep_pickup_datetime, tpep_dropoff_datetime) DESC
""").show()
```

## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

### Answer: 4040

## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay

### Answer: Governor's Island/Ellis Island/Liberty Island

### Code:

```python
!wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

df = spark.read \
    .option("header", "true") \
    .csv('taxi_zone_lookup.csv')

df.createOrReplaceTempView('taxi_zones')

spark.sql("""
SELECT
    tz.Zone,
    COUNT(1)
FROM
    trips_data td
    INNER JOIN taxi_zones tz
    ON td.PULocationID = tz.LocationID
WHERE
    trip_distance > 0
GROUP BY
    tz.Zone
ORDER BY
    COUNT(1)
""").show()
```