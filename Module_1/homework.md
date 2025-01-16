# Homework 1

## Question 1. Understanding docker first run 

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

## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

- postgres:5433
- localhost:5432
- db:5433
- postgres:5432
- db:5432

### The answer: postgres:5432 and db:5432

### Explanation:

- __postgres:5433__ is not correct because it's the port of the localhost and not available for pgadmin container.
- __localhost:5432__ is not correct because pgadmin is also running in a container, so the connection is not from localhost but from another container within the network.
- __db:5433__ is not correct for the same reason as postgres:5433, which is the port of the localhost and not available for the pgadmin container.
- __postgres:5432__ works because it's name of the container within the network.
- __db:5432__ works because it's name of the service within the network.


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

I wrote a python script to load the data to the PostgreSQL. By running these commands, the tables will be created in `postgres` database and the data will be loaded:
```bash
python load_to_postgres.py https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz postgres postgres mysecretpassword localhost 5432 green_tripdata
python load_to_postgres.py https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv postgres postgres mysecretpassword localhost 5432 zones
```

## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:
1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles 

Answers:

- 104,802;  197,670;  110,612;  27,831;  35,281
- 104,802;  198,924;  109,603;  27,678;  35,189
- 104,793;  201,407;  110,612;  27,831;  35,281
- 104,793;  202,661;  109,603;  27,678;  35,189
- 104,838;  199,013;  109,645;  27,688;  35,202

### The answer: 104,802;  198,924;  109,603;  27,678;  35,189

### Explanation:

```sql
select 
	count(*) filter (where gt.trip_distance <= 1) up_to_mile,
	count(*) filter (where gt.trip_distance > 1 and gt.trip_distance <= 3) between_1_3_miles,
	count(*) filter (where gt.trip_distance > 3 and gt.trip_distance <= 7) between_3_7_miles,
	count(*) filter (where gt.trip_distance > 7 and gt.trip_distance <= 10) between_7_10_miles,
	count(*) filter (where gt.trip_distance > 10) over_10_miles
from 
	public.green_tripdata gt 
where 
	extract(month from gt.lpep_pickup_datetime::date) = 10
	and extract(month from gt.lpep_dropoff_datetime::date) = 10
	and extract(year from gt.lpep_pickup_datetime::date) = 2019;
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

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](../../../01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.

### Solution

I completed the following steps:
- created a service account with appropriate permissions.
- generated the key and downloaded it in JSON format.
- created a VM on GCP.
- sent to the VM the JSON file generated earlier via SCP.
- connected via SSH to the VM.
- cloned the repository mentioned above.
- edited bucket name in `variables.tf`.
- ran the following commands and ensured that everything worked as expected:
	- `terraform init`,
	- `terraform validate`,
	- `terraform plan -var="project=<my-project-id>" -var="credentials=<path/to/my-key-json>.json"`,
	- `terraform apply -var="project=<my-project-id>" -var="credentials=<path/to/my-key-json>.json"`,
	- `terraform destroy -var="project=<my-project-id>" -var="credentials=<path/to/my-key-json>.json"`.


## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for: 
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:
- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-aprove, terraform destroy
- terraform init, terraform apply -auto-aprove, terraform destroy
- terraform import, terraform apply -y, terraform rm

### The answer: terraform init, terraform apply -auto-approve, terraform destroy

### Explanation: 

Such commands as `import`, `run` and `rm` don't exist in Terraform.