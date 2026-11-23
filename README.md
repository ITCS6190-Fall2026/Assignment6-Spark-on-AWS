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

### 4. S3: one bucket, three prefixes

Create a bucket named `ridecity-<your student ID>` in `us-east-1`, with the default
settings (public access blocked). Upload:

- `data/trips_history.csv` and `data/zones.csv` to `raw/`
- `glue_job.py` to `scripts/`

### 5. IAM: a role for Glue

In IAM, create a role named `ridecity-glue-role`: trusted entity **AWS service**, use case
**Glue**, with the policies `AWSGlueServiceRole` and `AmazonS3FullAccess`. The second one
is broader than needed; the report asks you how you would restrict it.

### 6. Glue: run the job

In AWS Glue, **ETL jobs**, **Script editor**, engine **Spark**, **Upload script** and
choose your `glue_job.py`. Then, in **Job details**:

| Setting | Value |
| ------- | ----- |
| Name | `ridecity-a6` |
| IAM Role | `ridecity-glue-role` |
| Glue version | 5.0 or later |
| Worker type, number of workers | G.1X, 2 |
| Job timeout | 10 minutes |
| Number of retries | 0 |
| Job parameters (under Advanced properties) | `--input` = `s3://ridecity-<your student ID>/raw` and `--output` = `s3://ridecity-<your student ID>/processed` |

Save and **Run**. In the **Runs** tab, wait for **Succeeded** (two to three minutes) and
open the **Output logs**: the job prints `trips written: 60000   zone_daily rows: 750` and
your total revenue. Write down the duration of the run.

In S3, `processed/trips/` now has thirty `pickup_date=...` folders. Select the `raw/`
prefix, then **Actions**, **Calculate total size**; do the same for `processed/`. Write
down the two sizes.

If the run fails, read the **Error logs** link of the run. The usual causes are a wrong
path in the job parameters, the wrong role, or the wrong region.

### 7. Glue: catalog the result

In AWS Glue, **Crawlers**, create `ridecity-crawler`: data source
`s3://ridecity-<your student ID>/processed/`, role `ridecity-glue-role`, target database
`ridecity_db` (create it from the same screen), on demand. Run it. It must create two
tables, `trips` and `zone_daily`. Open `trips` and look at its schema: `pickup_date` is
listed as a **partition key**, of type string.

### 8. Athena: three queries and one experiment

Open Athena. The first time, set the query result location to
`s3://ridecity-<your student ID>/athena-results/` (Settings). Choose the database
`ridecity_db`.

Run the three queries of `athena/q1.sql`, `q2.sql` and `q3.sql` (in `q3.sql`, replace
`2026-09-DD` with your day first). For each one, use **Download results** to save the
result as `results/q1.csv`, `results/q2.csv` and `results/q3.csv`, and write down the
**Data scanned** value Athena shows.

Then the experiment. Run these two queries and write down the **Data scanned** value of each:

```sql
SELECT ROUND(SUM(fare), 2) FROM trips;
SELECT ROUND(SUM(fare), 2) FROM trips WHERE pickup_date = '<your day, as 2026-09-DD>';
```

### 9. Clean up

Delete, in this order: the Glue job, the crawler, the database `ridecity_db` (with its
tables), the bucket (empty it first) and the role. Then take the last screenshot: the S3
bucket list without your bucket.

### 10. Report and submit

Fill in `REPORT.md`, put the five screenshots in `screenshots/`, commit and push. Submit the
link to your repository on Canvas.

**Screenshots** (your account name or ID visible at the top right in each):

1. `1_glue_run.png`: the Runs tab of the job with the Succeeded run, showing its duration.
2. `2_s3_partitions.png`: `processed/trips/` in S3 with the `pickup_date=...` folders.
3. `3_tables.png`: the two tables in `ridecity_db`.
4. `4_athena_q3.png`: query 3 in Athena with its result and Data scanned.
5. `5_cleanup.png`: the S3 bucket list after the cleanup.
