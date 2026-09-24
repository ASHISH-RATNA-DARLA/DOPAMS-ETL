import re

with open('etl_mo_seizures/etl_mo_seizure.py', 'r') as f:
    content = f.read()

# Replace any mention of mo_seizures having media column
content = re.sub(
    r"pos_longitude, media,\n",
    "pos_longitude,\n",
    content
)
content = re.sub(
    r"%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\n",
    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\n",
    content
)
content = re.sub(
    r"pos_longitude = EXCLUDED\.pos_longitude,\n\s+media = EXCLUDED\.media,",
    "pos_longitude = EXCLUDED.pos_longitude,",
    content
)
content = re.sub(
    r"seizure\['pos_longitude'\],\n\s+seizure\['media'\],\n\s+seizure\['mo_media_url'\]",
    "seizure['pos_longitude'],\n                    seizure['mo_media_url']",
    content
)

with open('etl_mo_seizures/etl_mo_seizure.py', 'w') as f:
    f.write(content)
