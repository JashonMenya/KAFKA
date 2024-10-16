import os
import pandas as pd
from confluent_kafka import Producer, Consumer, TopicPartition

def read_parquet_files(parquet_folder="parquets"):
    """
    Reads all Parquet files in the specified folder and prints the data.
    
    :param parquet_folder: The folder where Parquet files are stored.
    :return: None
    """
    # Get the current working directory
    project_dir = os.getcwd()

    # Full path to the parquet folder
    parquet_dir = os.path.join(project_dir, parquet_folder)

    # Check if the folder exists
    if not os.path.exists(parquet_dir):
        print(f"Directory {parquet_dir} does not exist.")
        return

    # Get all parquet files in the directory
    parquet_files = [f for f in os.listdir(parquet_dir) if f.endswith('.parquet')]

    if not parquet_files:
        print("No Parquet files found.")
        return

    # Loop through all Parquet files and print their content
    for parquet_file in parquet_files:
        file_path = os.path.join(parquet_dir, parquet_file)
        print(f"\nReading file: {file_path}")
        try:
            df = pd.read_parquet(file_path)
            print(df)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")




def get_output_path(folder_name="parquets"):
    """
    Returns the output location for stream data.
    
    If the specified folder does not exist, it creates the folder inside the 
    current project directory and returns the full path for storing Parquet files.
    
    :param folder_name: Name of the folder for storing the Parquet files.
    :return: Full path to the output folder for storing Parquet files.
    """
    project_dir = os.path.join(os.getcwd())
    output_dir = os.path.join(project_dir, folder_name)
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Directory {output_dir} created")
    
    return os.path.join(output_dir, "stream_data_")


def save_offsets(partition_number, offset_position):
    """
    Saves the latest offset position for a specific partition into a checkpoint file.
    
    The function updates the offset for the specified partition and writes 
    it into the checkpoint file to track the latest processed messages.
    
    :param partition_number: The partition number for which the offset is being saved.
    :param offset_position: The latest offset position to be saved.
    :return: None
    """
    offsets = load_offsets() or {}
    offsets[partition_number] = offset_position
    with open('checkpoint.txt', 'w') as checkpoint_file:
        for partition, offset in offsets.items():
            checkpoint_file.write(f"{partition}:{offset}\n")


def load_offsets():
    """
    Loads the latest saved offsets from the checkpoint file.
    
    Reads the checkpoint file to retrieve the last saved offsets for each partition.
    Returns a dictionary where the key is the partition number and the value is the 
    latest offset position.

    :return: Dictionary containing partition numbers and their latest offsets.
    :rtype: dict or None if file is not found.
    """
    try:
        offsets = {}
        with open('checkpoint.txt', 'r') as checkpoint_file:
            for line in checkpoint_file:
                partition, offset = line.strip().split(':')
                offsets[int(partition)] = int(offset)
        return offsets
    except FileNotFoundError:
        return None


def load_config():
    """
    Loads the Kafka client configuration from the 'client.properties' file.
    
    Reads the client properties file and parses it into a key-value dictionary 
    containing Kafka client configurations.
    
    :return: Dictionary containing Kafka configuration properties.
    :rtype: dict
    """
    kafka_config = {}
    with open("client.properties") as config_file:
        for line in config_file:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                kafka_config[parameter] = value.strip()
    return kafka_config


def produce_sample_message(topic_name, kafka_config):
    """
    Produces a sample message to a specified Kafka topic.
    
    This function creates a Kafka producer instance and sends a simple 
    key-value message to the provided topic.

    :param topic_name: The name of the Kafka topic to produce the message to.
    :param kafka_config: Dictionary containing Kafka client configurations.
    :return: None
    """
    producer = Producer(kafka_config)

    # Produce a sample message
    sample_key = "key"
    sample_value = "value"
    producer.produce(topic_name, key=sample_key, value=sample_value)
    print(f"Produced message to topic {topic_name}: key = {sample_key:12} value = {sample_value:12}")

    # Flush any outstanding or buffered messages to the Kafka broker
    producer.flush()


def consume_messages(topic_name, kafka_config, output_path):
    """
    Consumes messages from a Kafka topic, converts them to a DataFrame, and writes to Parquet files.
    
    This function reads messages from the specified Kafka topic, tracks offsets,
    and saves the messages to Parquet files after accumulating a specified number of messages.
    It also handles reconnecting at the last saved offset position for each partition.

    :param topic_name: The name of the Kafka topic to consume messages from.
    :param kafka_config: Dictionary containing Kafka client configurations.
    :param output_path: The path where the Parquet files should be stored.
    :return: None
    """
    kafka_config["group.id"] = "python-group-7"
    kafka_config["auto.offset.reset"] = "earliest"
    kafka_config["enable.auto.commit"] = "false"

    consumer = Consumer(kafka_config)
    consumer.subscribe([topic_name])

    # Load previously saved offsets (for all partitions)
    saved_offsets = load_offsets()

    # Ensure partitions are assigned to the consumer before seeking
    consumer.poll(0)  # Trigger assignment by polling
    assigned_partitions = consumer.assignment()

    if saved_offsets and assigned_partitions:
        # Seek to the saved offset for each partition
        for partition in assigned_partitions:
            if partition.partition in saved_offsets:
                tp = TopicPartition(topic_name, partition.partition, saved_offsets[partition.partition])
                consumer.seek(tp)

    # A list to hold consumed data before converting it to a DataFrame
    message_data = []

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("No message received in this poll.")
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            message_key = msg.key().decode("utf-8") if msg.key() is not None else None
            message_value = msg.value().decode("utf-8")
            partition_number = msg.partition()

            print(f"Consumed message from topic {topic_name}: key = {message_key} value = {message_value} from partition {partition_number}")

            # Append the message to the list (simulating streaming data)
            message_data.append({"key": message_key, "value": message_value, "partition": partition_number})

            # Convert the data list to a DataFrame every 10 messages
            if len(message_data) >= 10:
                df = pd.DataFrame(message_data)
                file_path = f"{output_path}{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}.parquet"
                df.to_parquet(file_path, index=False)
                print(f"DataFrame written to {file_path}")

                # Clear data after saving
                message_data = []

            # Save the current offset per partition
            save_offsets(msg.partition(), msg.offset() + 1)  # Pass partition and offset

    except KeyboardInterrupt:
        # Flush remaining data to Parquet
        if message_data:
            df = pd.DataFrame(message_data)
            file_path = f"{output_path}{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}.parquet"
            df.to_parquet(file_path, index=False)
            print(f"DataFrame written to {file_path}")

    finally:
        consumer.close()


