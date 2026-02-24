import requests
import json
import time

# API endpoint base
API_URL = "http://130.166.41.39:8000/consumer"

# Paths and topic
private_key_path = r"D:/School/Research/Current Research/NASA-JPL Digital Twin Interopability/Private Key/cadreA_omni_producer_agent.pem"
topic = "martin_viper_323232"
output_file_path = r"D:/School/Research/Current Research/NASA-JPL Digital Twin Interopability/hsml_schema/scripts/BL_plugins_HSML_API/Omni/kafkaOmniConsumer_1.json"

# Step 0: Read private key
try:
    private_key_file = open(private_key_path, "rb")
except Exception as e:
    print(f"Error reading private key: {e}")
    exit(1)

# Step 1: Authorize
auth_response = requests.post(
    f"{API_URL}/authorize",
    params={"topic": topic},
    files={"private_key": private_key_file}
)
print("Authentication:")
print(auth_response.json())

# Step 2: Start consumer
print("Consumer start")
start_response = requests.post(f"{API_URL}/start", params={"topic": topic})
print(start_response.json())

# Step 3: Poll and fetch latest HSML message
print("Polling latest messages from HSML API. Press Ctrl+C to stop.\n")
try:
    while True:
        response = requests.get(f"{API_URL}/latest-message", params={"topic": topic})
        if response.status_code == 200:
            hsml_data = response.json()

            with open(output_file_path, 'w', encoding='utf-8') as f:
                json.dump(hsml_data, f, indent=4)
                print(f"Message written to: {output_file_path}")
        else:
            print("No new message yet or error fetching.")

        time.sleep(0.01)  # Poll every 10 milliseconds
except KeyboardInterrupt:
    print("Stopped by user.")

# Step 4: Stop consumer
stop_response = requests.post(f"{API_URL}/stop", params={"topic": topic})
print(stop_response.json())
