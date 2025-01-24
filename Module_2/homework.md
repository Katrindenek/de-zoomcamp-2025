## Module 2 Workflow Orchestration Homework

In this homework I orchestrate workflow that extracts [New York Taxi Trip Data](https://github.com/DataTalksClub/nyc-tlc-data/releases) `.csv` files from GitHub and loads the data into the storage. 

In this case, the storage solutions used are Google Cloud Storage and BigQuery. The orchestration tool is [Kestra](https://kestra.io/).

[The flow code](gcp_taxi_scheduled.yaml) in Kestra stayed almost the same as it was shown in the [videos of the course](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/02-workflow-orchestration). Yet I added two more steps to clean staging (external) and processed tables generated each month for each type of taxi:
```yaml
  - id: bq_drop_ext
    type: io.kestra.plugin.gcp.bigquery.DeleteTable
    projectId: "{{kv('GCP_PROJECT_ID')}}"
    dataset: "{{kv('GCP_DATASET')}}"
    table: "{{render(vars.table_wo_dataset)}}_ext"

  - id: bq_drop
    type: io.kestra.plugin.gcp.bigquery.DeleteTable
    projectId: "{{kv('GCP_PROJECT_ID')}}"
    dataset: "{{kv('GCP_DATASET')}}"
    table: "{{render(vars.table_wo_dataset)}}"
```

### Quiz Questions

Complete the Quiz shown below. It’s a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra and ETL pipelines for data lakes and warehouses.

1) Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?
- 128.3 MB
- 134.5 MB
- 364.7 MB
- 692.6 MB

#### The answer: 128.3 MB

#### Explanation: 
This information can be found in the outputs of the task `extract` in Kestra flow. But then the task `purge_files` should be disabled. The other way is to check the size of that file in Google Cloud Storage Bucket.

---

2) What is the value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?
- `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv` 
- `green_tripdata_2020-04.csv`
- `green_tripdata_04_2020.csv`
- `green_tripdata_2020.csv`

#### The answer: `green_tripdata_2020-04.csv`

#### Explanation: The template will be populated with the corresponding input values.

---

3) How many rows are there for the `Yellow` Taxi data for the year 2020?
- 13,537.299
- 24,648,499
- 18,324,219
- 29,430,127

#### The answer: 24,648,499

#### Explanation: 

```sql
SELECT COUNT(*)
FROM `demo_dataset.yellow_tripdata`
WHERE filename LIKE '%2020%';
```

---

4) How many rows are there for the `Green` Taxi data for the year 2020?
- 5,327,301
- 936,199
- 1,734,051
- 1,342,034

#### The answer: 1,734,051

#### Explanation:

```sql
SELECT COUNT(*)
FROM `demo_dataset.green_tripdata`
WHERE filename LIKE '%2020%';
```

---

5) How many rows are there for the `Yellow` Taxi data for March 2021?
- 1,428,092
- 706,911
- 1,925,152
- 2,561,031

#### The answer: 1,925,152

#### Explanation:

```sql
SELECT COUNT(*)
FROM `demo_dataset.yellow_tripdata`
WHERE filename LIKE '%2021-03%';
```

---

6) How would you configure the timezone to New York in a Schedule trigger?
- Add a `timezone` property set to `EST` in the `Schedule` trigger configuration  
- Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration
- Add a `timezone` property set to `UTC-5` in the `Schedule` trigger configuration
- Add a `location` property set to `New_York` in the `Schedule` trigger configuration  

#### The answer: Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration

#### Explanation: 

According to the [Kestra Docs](https://kestra.io/plugins/core/triggers/trigger/io.kestra.plugin.core.trigger.schedule#timezone), to configure the timezone, one should use the `timezone` property of the Schedule trigger and set it to a value from the `TZ identifier` column in [Wiki](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List).