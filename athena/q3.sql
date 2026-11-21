-- Query 3: on YOUR day (datagen.py prints it), the busiest pickup hour of each borough.
-- Table: trips, filtered on pickup_date (the partition column)
-- Columns, in this order: borough, pickup_hour, trips
-- One row per borough: the hour with the most trips that day. If two hours tie, keep the
-- earlier one. Use a window function (ROW_NUMBER or RANK). Order: borough. Five rows.

