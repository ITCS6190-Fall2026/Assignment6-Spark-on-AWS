-- Query 1: the five pickup zones with the highest revenue in the month.
SELECT zone_name, borough, SUM(trips) AS trips, ROUND(SUM(revenue), 2) AS revenue
FROM zone_daily
GROUP BY zone_name, borough
ORDER BY revenue DESC
LIMIT 5;
