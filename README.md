# Assignment #6 (optional): Spark on AWS

**ITCS 6190/8190 - Cloud Computing for Data Analysis - Fall 2026**

All semester you ran Spark on a cluster on your own machine. In this assignment you run the
same kind of job on AWS, with the services you met in the two Coursera courses: the data
goes to **S3**, a Spark job runs on **AWS Glue** (serverless Spark: no cluster to manage)
and writes Parquet, a **Glue crawler** puts the result in the Data Catalog, and you query
it with SQL in **Athena**. The data is the RideCity trips of Assignment #4, so you already
know it.

**There is no code to write.** The Spark job and the SQL queries are given. Your work is on
AWS: storage, permissions, running the job, the catalog, the queries, the costs and the
cleanup.

---

## Before you start

- **An AWS account on the free plan.** New accounts get credits and cannot be charged while
  on the free plan; S3, Glue and Athena are included. Signing up asks for a card and a
  phone number. The whole assignment uses well under 1 dollar of credits.
- **One region for everything: US East (N. Virginia), `us-east-1`.** Check the region
  selector at the top right of the console before each step.
- **Everything is done in the AWS console.** You do not need access keys, so do not create
  any, and never commit a key or a password to a repository.
- **Clean up at the end** (step 9). It is graded.

If you use an AWS Academy Learner Lab instead of your own account: skip step 5 and choose
the existing role `LabRole` wherever a role is asked. The rest is the same.

---

## What is in this repository

| Path | What it is |
| ---- | ---------- |
| `datagen.py` | generates the trips and the zones from a seed (plain Python) |
| `glue_job.py` | the Spark job, complete: it joins the trips with the zones and writes two Parquet datasets |
| `athena/q1.sql`, `q2.sql`, `q3.sql` | the three queries you run in Athena |
| `results/` | where you save the result of each query, downloaded from Athena |
| `screenshots/` | the five screenshots the report asks for |
| `REPORT.md` | the report template you fill in |

---

## Steps

### 1. Make your own copy of this repository

Click **Use this template**, then **Create a new repository**. Name it
`ITCS6190-A6-<your-name>` and set the visibility to **Public**. Clone *your* repository.

### 2. Generate your data

```bash
python3 datagen.py <your student ID>
```

It writes `data/trips_history.csv` (60,000 trips of September 2026) and `data/zones.csv`
(25 zones), and prints **your day**, which query 3 uses. Write it down. The `data/` folder
is not committed.

### 3. Read the job

Open `glue_job.py`. It is a plain PySpark script, the same code you would give to
`spark-submit`, with nothing specific to Glue. It reads the two CSV files from `--input`,
joins the trips with the zones, and writes to `--output`:

- `trips/`: one row per trip, in Parquet, **partitioned by `pickup_date`** (one folder per day)
- `zone_daily/`: one row per day and pickup zone, with trips, revenue and averages

If you have PySpark on your machine (or the spark-master container of any hands-on), you
can try it first, but this is optional:

```bash
python3 glue_job.py --input data --output output
```

### 4. S3: one bucket, one folder

In the S3 console, create a bucket named `ridecity-<your student ID>` in `us-east-1`, with
the default settings (public access blocked). Open the bucket, click **Create folder** and
name it `raw`. Open it, click **Upload**, and add the two files `datagen.py` generated on
your machine: `data/trips_history.csv` and `data/zones.csv`.

The bucket now contains `raw/trips_history.csv` and `raw/zones.csv`. The job will create
`processed/` itself.

### 5. IAM: a role for Glue

In IAM, create a role named `ridecity-glue-role`: trusted entity **AWS service**, use case
**Glue**, with the policies `AWSGlueServiceRole` and `AmazonS3FullAccess`. The second one
is broader than needed; the report asks you how you would restrict it.

### 6. Glue: run the job

Check the region selector first: it must show the region of your bucket. Then, in AWS Glue,
**ETL jobs**, **Script editor**: engine **Spark**, option **Upload script**,
**Choose file** and pick `glue_job.py` from your machine, then **Create script**. Then open the **Job details** tab:

| Setting | Value |
| ------- | ----- |
| Name | `ridecity-a6` |
| IAM Role | click **Customize**, under the description, to show the role list, and choose `ridecity-glue-role` |
| Glue version | 5.0 or later |
| Worker type, number of workers | G.1X, 2 |
| Job timeout | 10 minutes |
| Number of retries | 0 |
| Job parameters (at the end of Advanced properties) | two parameters, with **Add new parameter**: key `--input`, value `s3://ridecity-<your student ID>/raw`; key `--output`, value `s3://ridecity-<your student ID>/processed`. The keys include the two dashes. |

**Save**, then **Run**. In the **Runs** tab, wait for **Succeeded** (one to three minutes)
and select the run. In the **Run details** panel below, note the **Execution time**, and
under **Cloudwatch logs** click **Output logs**: the log stream named after the run shows
what the job printed, `trips written: 60000   zone_daily rows: 750` and your total revenue.
(The **Logs** panel next to Run details shows the messages of Spark and Glue, not these
two lines.)

In S3, `processed/trips/` now has thirty `pickup_date=...` folders. Select the `raw/`
prefix, then **Actions**, **Calculate total size**; do the same for `processed/`. Write
down the two sizes.

If the run fails, read the **Logs** panel of the run. The usual causes: the job
parameters are missing (`the following arguments are required: --input, --output`), a
path is wrong, or the role was not selected (`Role should not be null or empty`).

### 7. Glue: catalog the result

In AWS Glue, open the left menu (the icon at the top left, if it is collapsed) and choose
**Crawlers**, under Data Catalog. Create `ridecity-crawler`: data source
`s3://ridecity-<your student ID>/processed/`, role `ridecity-glue-role`, target database
`ridecity_db` (create it from the same screen), on demand. Run it. It must create two
tables, `trips` and `zone_daily`. Open `trips` and look at its schema: `pickup_date` is
listed as a **partition key**, of type string.

### 8. Athena: three queries and one experiment

Open Athena. The first time, set the query result location to
`s3://ridecity-<your student ID>/athena-results/` (Settings). Choose the database
`ridecity_db`.

Run the three queries, one at a time: open `athena/q1.sql`, copy its content into the
Athena query editor and click **Run**; then `q2.sql`; then `q3.sql`.

Query 3 is about **your day**, the date `datagen.py` printed in step 2 (for example
`your day for query 3: 2026-09-19`). The file contains the placeholder `2026-09-DD`:
before running it, replace `DD` with the two digits of your day, so that the line reads,
in this example, `WHERE pickup_date = '2026-09-19'`. Save the file with your day in it.

For each query, use **Download results** to save the result as `results/q1.csv`,
`results/q2.csv` and `results/q3.csv`, and write down the **Data scanned** value Athena
shows.

Then the experiment. Run these two queries and write down the **Data scanned** value of each:

```sql
SELECT ROUND(SUM(fare), 2) FROM trips;
SELECT ROUND(SUM(fare), 2) FROM trips WHERE pickup_date = '2026-09-DD';   -- DD: your day
```

### 9. Clean up

Delete, in this order: the Glue job, the crawler, the database `ridecity_db` (with its
tables), your bucket (empty it first), the bucket Glue created for the script and its logs
(its name starts with `aws-glue-assets`; empty it first) and the role. Then take the last
screenshot: the S3 bucket list without the two buckets.

### 10. Report and submit

Fill in `REPORT.md`, put the five screenshots in `screenshots/`, commit and push. Submit the
link to your repository on Canvas.

**Screenshots** (your account name or ID visible at the top right in each):

1. `1_glue_run.png`: the Runs tab of the job with the Succeeded run, showing its duration.
2. `2_s3_partitions.png`: `processed/trips/` in S3 with the `pickup_date=...` folders.
3. `3_tables.png`: the two tables in `ridecity_db`.
4. `4_athena_q3.png`: query 3 in Athena with its result and Data scanned.
5. `5_cleanup.png`: the S3 bucket list after the cleanup.
