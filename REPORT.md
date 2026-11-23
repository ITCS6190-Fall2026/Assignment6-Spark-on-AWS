# Assignment #6: Report

**Name:**
**Student ID:**
**Email:**

---

## Seed and day

Seed used for `datagen.py`:

Your day for query 3:

Account used (own account on the free plan, or Learner Lab):

---

## The Glue job

The two lines printed by the job on Glue (from the Output logs):

```

```

![Glue run](screenshots/1_glue_run.png)
![S3 partitions](screenshots/2_s3_partitions.png)

Duration of the run, number of workers, and your estimate of what the run cost (Glue bills
per DPU-hour; a G.1X worker is 1 DPU; look up the current price for `us-east-1`):



In the hands-ons you started a Spark cluster with Docker before every job. What did you
have to start or manage here, and what did Glue do for you? Name one thing you give up in
exchange.



## Catalog and Athena

![Tables](screenshots/3_tables.png)
![Query 3](screenshots/4_athena_q3.png)

Size of `raw/` and size of `processed/` in S3 (Calculate total size):



The result of each query is in `results/`. One sentence on what each result tells you, and
its Data scanned:

- Query 1:
- Query 2:
- Query 3:

The experiment: Data scanned by the sum of fares over all trips, and by the same sum for
your day only. Explain the difference: what did the job do that makes it possible, and how
does Athena use it?



Athena bills by data scanned. Give two reasons why these queries would cost more on the
original CSV file than on your Parquet tables.



## Security

The role used `AmazonS3FullAccess`. Write the IAM policy (JSON) that would give the job only
what it needs: read `raw/` and `scripts/`, write `processed/`, in your bucket only.

```json

```

## Cleanup

![Cleanup](screenshots/5_cleanup.png)

What you deleted, in order:



## Problems

Any error you hit on AWS and how you fixed it, or "none":


