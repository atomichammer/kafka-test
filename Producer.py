import os
import time
import shutil
import fnmatch
from kafka import KafkaProducer

# Define the Kafka topic to which the CSV data will be sent
topic = "demo_topic"

# Define the path to the directory to monitor for new files
input_dir = "input/"

# Define the path to the directory where processed files will be moved
output_dir = "processed/"

# Define the filename mask for files to monitor
filename_mask = "*.csv"

# Define the minimum age (in seconds) for files to consider for processing
min_file_age = 60

# Define the polling frequency (in seconds) for checking for new files
polling_freq = 10

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Continuously monitor the input directory for new files to process
while True:
    # Get a list of files in the input directory that match the filename mask
    files = [f for f in os.listdir(input_dir) if fnmatch.fnmatch(f, filename_mask)]
    # Process each file in the list
    for file in files:
        # Get the full path to the file
        file_path = os.path.join(input_dir, file)
        # Get the age of the file (in seconds)
        file_age = time.time() - os.path.getmtime(file_path)
        # If the file is old enough to process
        if file_age >= min_file_age:
            # Open the file and read it line by line
            with open(file_path, "r") as f:
                for line in f:
                    # Strip the newline character from the line
                    line = line.strip()
                    producer.send(topic, value=f"{line}".encode("utf-8"))
            # send the data to a kafka topic
            producer.send(topic, value=f"FILE_END".encode("utf-8"))
            # Move the processed file to the output directory
            processed_file_path = os.path.join(output_dir, file)
            shutil.move(file_path, processed_file_path)
            print(f"File {file} transferred successfully.")
    # Wait for the polling frequency before checking for new files again
    time.sleep(polling_freq)

# Close the Kafka producer
producer.close()