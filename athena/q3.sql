-- Query 3: on YOUR day, the busiest pickup hour of each borough.
-- Replace 2026-09-DD with your day (datagen.py prints it). pickup_date is the partition
-- column of the table, so Athena reads only the folder of that day.
SELECT borough, pickup_hour, trips
FROM (
  SELECT borough, pickup_hour, COUNT(*) AS trips,
         ROW_NUMBER() OVER (PARTITION BY borough ORDER BY COUNT(*) DESC, pickup_hour ASC) AS rn
  FROM trips
  WHERE pickup_date = '2026-09-DD'
  GROUP BY borough, pickup_hour
) t
WHERE rn = 1
ORDER BY borough;
