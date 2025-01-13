# Homework 1

## Question 1. Knowing docker tags

Which subcommand does this?

*Remove one or more images*

- `delete`
- `rc`
- `rmi`
- `rm`

### The answer: `rmi`.

### Explanation:

- `delete`: This is not a valid Docker subcommand.
- `rc`: This is also not a valid Docker subcommand.
- `rmi`: This is the correct Docker subcommand to remove one or more images.
- `rm`: This subcommand is used to remove one or more containers, not images.

## Question 2. Understanding docker first run 

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.

What's the version of `pip` in the image?

- 24.3.1
- 24.2.1
- 23.3.1
- 23.2.1

### The answer: `24.3.1`.

### Explanation:

To run the `python:3.12.8` image in interactive mode with the entrypoint `bash`, one can use the following command:

```bash
docker run -it --entrypoint bash python:3.12.8
```

Once inside the container, one can check the version of `pip` by running:

```bash
pip --version
```

The output will show the following result:
```
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
```

##  Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from October 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

You will also need the dataset with zones:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

Download this data and put it into Postgres.

You can use the code from the course. It's up to you whether
you want to use Jupyter or a python script.

### Solution

To start PostgreSQL in Docker, you can use the following command:

```bash
docker run -d -p 5432:5432 --name postgres_container -e POSTGRES_PASSWORD=mysecretpassword postgres
```

This command will:

- Pull the latest PostgreSQL image from Docker Hub if it is not already available locally.
- Start a new container named `some-postgres`.
- Set the PostgreSQL password to `mysecretpassword`.
- Run the container in detached mode (`-d`).
- Map ports of the localhost and the container `5432:5432`.

I wrote a python script to load the data to the PostgreSQL. By running these commands, the tables will be created in `postgres` database and the data will be loaded:
```bash
python load_to_postgres.py https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz postgres postgres mysecretpassword localhost 5432 green_tripdata
python load_to_postgres.py https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv postgres postgres mysecretpassword localhost 5432 zones
```

## Question 3. Count records 

How many taxi trips were made on October 18th, 2019?

(Trips that started and finished on that day) 

- 13417
- 15417
- 17417
- 19417

### The answer: 17417

### Explanation:

```sql
select
	count(*) as total_trips
from
	public.green_tripdata
where 
	lpep_pickup_datetime::date = '2019-10-18'
	and lpep_dropoff_datetime::date = '2019-10-18';
```


## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance. 

- 2019-10-11
- 2019-10-24
- 2019-10-26
- 2019-10-31

### The answer: 2019-10-31

### Explanation:

```sql
select 
	lpep_pickup_datetime::date,
	max(trip_distance::decimal) as max_trip_distance
from 
	public.green_tripdata gt 
group by
	lpep_pickup_datetime::date
order by
	max(trip_distance::decimal) desc
limit 1;
```

## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.
 
- East Harlem North, East Harlem South, Morningside Heights
- East Harlem North, Morningside Heights
- Morningside Heights, Astoria Park, East Harlem South
- Bedford, East Harlem North, Astoria Park

### The answer: East Harlem North, East Harlem South, Morningside Heights

### Explanation:

```sql
select
	z."Zone",
	SUM(gt.total_amount)
from
	public.green_tripdata gt
	left join public.zones z
	on gt."PULocationID" = z."LocationID" 
where 
	gt.lpep_pickup_datetime::date = '2019-10-18'
group by 
	z."Zone"
having
	SUM(gt.total_amount) > 13000
order by 
	SUM(gt.total_amount) desc;
```


## Question 6. Largest tip

For the passengers picked up in Ocrober 2019 in the zone
name "East Harlem North" which was the drop off zone that had
the largest tip?

Note: it's `tip` , not `trip`

We need the name of the zone, not the ID.

- Yorkville West
- JFK Airport
- East Harlem North
- East Harlem South

### The answer: JFK Airport

### Explanation:

```sql
select 
	zdo."Zone", 
	gt.tip_amount
from 
	public.green_tripdata gt
	left join public.zones zpu
	on gt."PULocationID" = zpu."LocationID" 
	left join public.zones zdo
	on gt."DOLocationID" = zdo."LocationID" 
where 
	zpu."Zone" = 'East Harlem North'
	and extract(month from gt.lpep_pickup_datetime::date) = 10
order by 
	gt.tip_amount desc
limit 1;
```