with open("data/processed/observations_clean.csv") as f:
    lines = f.readlines()

data = lines[1:]

temps = []
objects = []

for line in data:
    parts = line.strip().split(",")
    temp = float(parts[2])
    obj = parts[1]
    
    temps.append(temp)
    objects.append(obj)

avg_temp = sum(temps) / len(temps)

object_counts = {}
for obj in objects:
    object_counts[obj] = object_counts.get(obj, 0) + 1

print("=== DATA SUMMARY ===")
print("Total records:", len(data))
print("Average temperature:", round(avg_temp, 2))

print("\nObject occurrences:")
for obj, count in object_counts.items():
    print(f"{obj}: {count}")
