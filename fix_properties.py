import re

with open('etl-properties/etl_properties.py', 'r') as f:
    content = f.read()

# Remove media from INSERT
content = re.sub(
    r"category, additional_details, media,\n",
    "category, additional_details,\n",
    content
)
content = re.sub(
    r"%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\n",
    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\n",
    content
)
content = re.sub(
    r"additional_details = EXCLUDED\.additional_details,\n\s+media = EXCLUDED\.media,",
    "additional_details = EXCLUDED.additional_details,",
    content
)

# Remove media param from execute args
content = re.sub(
    r"prop\['additional_details'\],\n\s+prop\['media'\],\n\s+prop\['date_created'\]",
    "prop['additional_details'],\n                    prop['date_created']",
    content
)

# Remove call to upsert_property_additional_details
content = re.sub(
    r"\s+# Also write to the child table.*?self\.upsert_property_additional_details\([^\)]+\)",
    "",
    content,
    flags=re.DOTALL
)

# Remove def upsert_property_additional_details
content = re.sub(
    r"def upsert_property_additional_details\(.*?def replace_property_media\(",
    "def replace_property_media(",
    content,
    flags=re.DOTALL
)

with open('etl-properties/etl_properties.py', 'w') as f:
    f.write(content)
print("Updated etl_properties.py")
