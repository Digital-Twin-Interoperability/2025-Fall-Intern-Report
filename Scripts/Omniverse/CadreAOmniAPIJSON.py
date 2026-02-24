import requests
import json
import os
import time
from pathlib import Path
import tempfile

API_URL = "http://130.166.41.39:8000/producer"
private_key_path = r"D:/School/Research/Current Research/NASA-JPL Digital Twin Interopability/Private Key/cadreA_omni_producer_agent.pem"
#json_path = Path(tempfile.gettempdir()) / "producerOmniUpdate.json"
json_path = r"D:/School/Research/Current Research/NASA-JPL Digital Twin Interoperability/collision_ideas/code/producerOmniUpdate.json"
# topic = "omni_cadrea_producer_7cyt86" WRONG TOPIC
topic = "omni_cadrea_producer_t9glwc"

# Authenticate
with open(private_key_path, "rb") as key_file:
    auth_response = requests.post(
        f"{API_URL}/authenticate",
        params={"topic": topic},
        files={"private_key": key_file}
    )
    print("Authentication: ")
    print(auth_response.json())

last_sent = {}

print("Connecting to get topic")
while True:
    try:
        if os.path.exists(json_path):
            with open(json_path, 'r') as f:
                data = json.load(f)

            # Avoid re-sending same data
            if data == last_sent:
                time.sleep(0.01)
                continue

            hsml_message = {
                "@context": "https://digital-twin-interoperability.github.io/hsml-schema-context/hsml.jsonld",
                "@type": "Agent",
                "name": "Omni_CadreA_Producer",
                "creator": {
                "@type": "Person",
                "name": "Elijah",
                "swid": "did:key:6Mkpm86HYpbVbctEbgef74hby34ck9BDgQkCSm4hPt2FbAv"
                },
                "dateCreated": "2025-10-23",
                "dateModified": "2025-10-23",
                "description": "Agent: Cadre A, produced by Omniverse ",
                "swid": "did:key:6MksLzYr9CEkwZeAsaXe1nNEUeJ56pAiC6QwEZBjgC8SuZT",
                "url": "https://example.com/3dmodel",
                "encodingFormat": "application/x-obj",
                "contentUrl": "https://example.com/models/3dmodel-001.obj",
                "platform": "Omniverse",
                "inControl": True,
                "spaceLocation": [{"@type": "Hyperspace", "name": "Moon"}],
                "position": [
                    {"@type": "schema:PropertyValue", "name": "xCoordinate", "value": data["position"][0]},
                    {"@type": "schema:PropertyValue", "name": "yCoordinate", "value": data["position"][1]},
                    {"@type": "schema:PropertyValue", "name": "zCoordinate", "value": data["position"][2]}
                ],
                "rotation1": [
                    {"@type": "schema:PropertyValue", "name": "rx", "value": data["rotation1"][1]},
                    {"@type": "schema:PropertyValue", "name": "ry", "value": data["rotation1"][3]},
                    {"@type": "schema:PropertyValue", "name": "rz", "value": data["rotation1"][2]},
                    {"@type": "schema:PropertyValue", "name": "w", "value": data["rotation1"][0]}
                ],
                # "rotation": [
                    # {"@type": "schema:PropertyValue", "name": "rx", "value": data["rotation"][0]},
                    # {"@type": "schema:PropertyValue", "name": "ry", "value": data["rotation"][1]},
                    # {"@type": "schema:PropertyValue", "name": "rz", "value": data["rotation"][2]}
                # ],
                "additionalProperty": [
                    {"@type": "schema:PropertyValue", "name": "scale", "value": 100}
                ]
            }

            response = requests.post(f"{API_URL}/send-message", params={"topic": topic}, json=hsml_message)
            print(f"Message sent: {response.json()}")
            last_sent = data
        else:
            print("JSON doesn't exist lil bro")

    except Exception as e:
        print(f"Error: {e}")

    time.sleep(0.001)