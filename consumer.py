from confluent_kafka import Producer, Consumer, TopicPartition

def save_offsets(partition, offset):
    # Load existing offsets
    offsets = load_offsets() or {}

    # Update the offset for the given partition
    offsets[partition] = offset

    # Write all offsets back to the checkpoint file
    with open('checkpoint.txt', 'w') as f:
        for partition, offset in offsets.items():
            f.write(f"{partition}:{offset}\n")


def load_offsets():
    try:
        offsets = {}
        with open('checkpoint.txt', 'r') as f:
            for line in f:
                partition, offset = line.strip().split(':')
                offsets[int(partition)] = int(offset)
        return offsets
    except FileNotFoundError:
        return None


def read_config():
    # reads the client configuration from client.properties
    # and returns it as a key-value map
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config


def produce(topic, config):
    # creates a new producer instance
    producer = Producer(config)

    # produces a sample message
    key = "key"
    value = "value"
    producer.produce(topic, key=key, value=value)
    print(f"Produced message to topic {topic}: key = {key:12} value = {value:12}")

    # send any outstanding or buffered messages to the Kafka broker
    producer.flush()


# Auto saved offsets
# def consume(topic, config):
#     config["group.id"] = "python-group-6"  # Change group ID to force reading from start
#     config["auto.offset.reset"] = "earliest"
#     config["enable.auto.commit"] = "false"

#     consumer = Consumer(config)
#     consumer.subscribe([topic])

#     try:
#         while True:
#             msg = consumer.poll(1.0)
#             if msg is None:
#                 print("No message received in this poll.")
#                 continue

#             if msg.error():
#                 print(f"Consumer error: {msg.error()}")
#                 continue

#             key = msg.key().decode("utf-8") if msg.key() is not None else None
#             value = msg.value().decode("utf-8")
#             print(f"Consumed message from topic {topic}: key = {key} value = {value}")
#     except KeyboardInterrupt:
#         pass
#     finally:
#         consumer.close()

# Manually save offsets
def consume(topic, config):
    config["group.id"] = "python-group-6"
    config["auto.offset.reset"] = "earliest"
    config["enable.auto.commit"] = "false"

    consumer = Consumer(config)
    consumer.subscribe([topic])

    # Load previously saved offsets (for all partitions)
    saved_offsets = load_offsets()

    # Ensure partitions are assigned to the consumer before seeking
    consumer.poll(0)  # Trigger assignment by polling
    partitions = consumer.assignment()

    if saved_offsets and partitions:
        # Seek to the saved offset for each partition
        for partition in partitions:
            if partition.partition in saved_offsets:
                tp = TopicPartition(topic, partition.partition, saved_offsets[partition.partition])
                consumer.seek(tp)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("No message received in this poll.")
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() is not None else None
            value = msg.value().decode("utf-8")
            print(f"Consumed message from topic {topic}: key = {key} value = {value} from partition {msg.partition()}")

            # Save the current offset per partition
            save_offsets(msg.partition(), msg.offset() + 1)  # Pass partition and offset
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()



def main():
    config = read_config()
    topic = "benefitdetails"

    #   produce(topic, config)
    consume(topic, config)


main()
