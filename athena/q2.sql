-- Query 2: trips, revenue and average fare per payment type.
SELECT payment_type, COUNT(*) AS trips, ROUND(SUM(fare), 2) AS revenue, ROUND(AVG(fare), 2) AS avg_fare
FROM trips
GROUP BY payment_type
ORDER BY payment_type;
