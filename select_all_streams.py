"""select_all_streams.py — Mark every stream and field as selected in catalog."""
import json

with open("catalog_result1.json", encoding="utf-8") as f:
    catalog = json.load(f)

for stream in catalog["streams"]:
    for entry in stream.get("metadata", []):
        entry["metadata"]["selected"] = True

with open("catalog_selected_result1.json", "w", encoding="utf-8") as f:
    json.dump(catalog, f, indent=2)

print(f"Total streams selected: {len(catalog['streams'])}")
print("Saved to catalog_selected_menagerie.json")