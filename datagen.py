"""Assignment #6: data generator (plain Python, standard library only).

    python3 datagen.py <seed> [output directory]

Writes, under the output directory (default: data/ next to this file):

  trips_history.csv   60,000 completed RideCity trips of September 2026
  zones.csv           the 25 zones of the city, with name and borough

Use your student ID as the seed. The trips are the same as the ones Assignment #4 generated
for that seed, so you already know this data. The grader regenerates it from the seed you
write in your report.
"""
import csv, os, random, sys
from datetime import datetime, timedelta

if len(sys.argv) < 2:
    print(__doc__); sys.exit(2)
seed = int(sys.argv[1])
out = sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(out, exist_ok=True)
rnd = random.Random(seed)

# ---------------------------------------------------------------- zones
names = ["Downtown", "Uptown", "Midtown", "Harbor", "Airport", "University", "Old Town", "Riverside",
         "Tech Park", "Stadium", "Market", "Hillside", "Lakeview", "Eastgate", "Westgate", "Northfield",
         "Southport", "Greenwood", "Civic Center", "Arts District", "Ferry", "Parkside", "Mill", "Heights", "Bayfront"]
boroughs = ["Central", "North", "South", "East", "West"]
zones = [{"zone_id": i + 1, "zone_name": n, "borough": boroughs[i % 5]} for i, n in enumerate(names)]
with open(f"{out}/zones.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["zone_id", "zone_name", "borough"]); w.writeheader(); w.writerows(zones)

# the pricing model of Assignment #4, unchanged, so the same seed gives the same trips
zone_premium = {z["zone_id"]: 0.0 for z in zones}
zone_premium[5] = 9.0          # Airport
zone_premium[1] = 2.5          # Downtown
zone_premium[21] = 4.0         # Ferry
pay_extra = {"card": 0.0, "app": 0.0, "cash": 1.0}
zone_weights = [6 if z["zone_id"] in (1, 3, 5) else 1 + (z["zone_id"] % 3) for z in zones]

def fare_of(distance, duration, zone, hour, payment):
    surge = 1.25 if hour in (7, 8, 17, 18) else 1.0
    return (2.5 + 1.6 * distance + 0.35 * duration) * surge + zone_premium[zone] + pay_extra[payment]

def trip(ts):
    zone = rnd.choices(zones, weights=zone_weights, k=1)[0]["zone_id"]
    distance = round(max(0.3, rnd.lognormvariate(1.4, 0.6)), 2)
    duration = round(distance * rnd.uniform(2.2, 4.0) + rnd.uniform(1, 4), 1)
    payment = rnd.choices(["card", "app", "cash"], weights=[5, 4, 1], k=1)[0]
    fare = round(fare_of(distance, duration, zone, ts.hour, payment) + rnd.gauss(0, 1.5), 2)
    return zone, distance, duration, payment, max(3.0, fare)

# ---------------------------------------------------------------- history
N_TRIPS, N_DRIVERS = 60000, 250
base = datetime(2026, 9, 1)
with open(f"{out}/trips_history.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["trip_id", "driver_id", "pickup_zone", "dropoff_zone", "pickup_ts", "distance_km",
                "duration_min", "payment_type", "fare"])
    for i in range(1, N_TRIPS + 1):
        ts = base + timedelta(seconds=rnd.randint(0, 30 * 24 * 3600 - 1))
        zone, distance, duration, payment, fare = trip(ts)
        w.writerow([f"T{i:06d}", f"D{rnd.randint(1, N_DRIVERS):03d}", zone, rnd.randint(1, 25),
                    ts.strftime("%Y-%m-%d %H:%M:%S"), distance, duration, payment, fare])

day = seed % 30 + 1
print(f"seed {seed}: {N_TRIPS} trips and {len(zones)} zones written to {out}")
print(f"your day for query 3: 2026-09-{day:02d}")
