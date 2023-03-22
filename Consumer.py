import os
import time
from datetime import datetime
from kafka import KafkaConsumer

# Define the Kafka topic from which the CSV data will be consumed
topic = "demo_topic"

# Create a Kafka consumer instance
consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092')

# Initialize the CSV data and checksum variables
csv_data = ""

output_dir = "received/"

# while True:
# Consume messages from the Kafka topic
for message in consumer:
    # Decode the message from bytes to string
    message_str = message.value.decode("utf-8")
    if not message_str.startswith("FILE_END"):
        csv_data += message_str + "\n"
    else:
        timestamp = datetime.now().strftime('%d%m%Y%H%M%S')
        filename = f'{timestamp}.csv'
        file_path = os.path.join(output_dir, filename)
        print(file_path)
        with open(file_path, "w") as f:
            f.write(csv_data)
        print("File transferred successfully.")

# Close the Kafka consumer
consumer.close()