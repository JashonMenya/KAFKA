

from helper_utils import consume_messages, get_output_path, load_config


def main():
    """
    Main function that orchestrates the Kafka message consumption and saving them to Parquet files.
    
    This function initializes the Kafka configuration, sets the output directory, 
    and triggers the consumption process for the given Kafka topic.
    
    :return: None
    """
    kafka_config = load_config()
    topic_name = "your_topic_name"

    output_path = get_output_path("parquets")

    # Consume messages and write them to Parquet
    consume_messages(topic_name, kafka_config, output_path)


main()
