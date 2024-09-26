import logging
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

def create_keyspace(session):
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS property_streams
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    print("Keyspace created successfully!")
    logging.info("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS property_streams.properties (
        price text, 
        title text,
        link text,
        data_extracted text,
        pictures list<text>,
        address text,
        bedrooms text,
        bathrooms text,
        receptions text,
        epc_rating text,
        tenure text,
        time_remaining_on_lease text,
        service_charge text,
        council_tax_band text,
        ground_rent text,
        PRIMARY KEY (link)
    );
    """)
    print("Table created successfully!")
    logging.info("Table created successfully!")

def insert_data(session, **kwargs):
    logging.info("Insert data successfully!")
    # Define defaults for each field, using `None` for fields that can be `NULL` in the database
    defaults = {
        'price': None,
        'title': None,
        'link': 'https://www.zoopla.co.uk/',
        'data_extracted': None,
        'pictures': None,  # Ensure this can handle `NULL` if no list is provided
        'address': None,
        'bedrooms': None,
        'bathrooms': None,
        'receptions': None,
        'epc_rating': None,
        'tenure': None,
        'time_remaining_on_lease': None,
        'service_charge': None,
        'council_tax_band': None,
        'ground_rent': None
    }

    try:
         # Update the defaults with actual values provided, missing values remain as defaults
        final_data = {field: kwargs.get(field, defaults[field]) for field in defaults}
    
        logging.info(f"Inserting data with fields: {', '.join(final_data.keys())}")
        # Using tuple unpacking for the values during the insert
        session.execute("""
            INSERT INTO property_streams.properties(price, title, link, data_extracted, pictures, address, bedrooms, bathrooms, receptions, epc_rating, tenure, time_remaining_on_lease, service_charge, council_tax_band, ground_rent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(final_data.values()))
        logging.info("Data inserted successfully!")
    except Exception as e:
        logging.error(f"Failed to insert data: {str(e)}")


# def insert_data(session, **kwargs):
#     required_fields = ['price', 'title', 'link', 'pictures', 'address', 'bedrooms', 'bathrooms', 'receptions', 'epc_rating', 'tenure', 'time_remaining_on_lease', 'service_charge', 'council_tax_band', 'ground_rent']
#     if not all(field in kwargs for field in required_fields):
#         logging.error("Missing data fields, not inserting to Cassandra")
#         return

#     logging.info("Inserting data " + str(kwargs))
#     try:
#         session.execute("""
#             INSERT INTO property_streams.properties(price, title, link, data_extracted, pictures, address, bedrooms, bathrooms, receptions, epc_rating, tenure, time_remaining_on_lease, service_charge, council_tax_band, ground_rent)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, (kwargs['price'], kwargs['title'], kwargs['link'], kwargs['data_extracted'], kwargs['pictures'], kwargs['address'], kwargs['bedrooms'], kwargs['bathrooms'], kwargs['receptions'], kwargs['epc_rating'], kwargs['tenure'], kwargs['time_remaining_on_lease'], kwargs['service_charge'], kwargs['council_tax_band'], kwargs['ground_rent']))
#         print("Data inserted successfully!")
#         logging.info("Data inserted successfully!")
#     except Exception as e:
#         print(f"Failed to insert data: {str(e)}")
#         logging.error(f"Failed to insert data: {str(e)}")


    # logging.info("Inserting data")
    # print("Inserting data")
    # session.execute("""
    #     INSERT INTO property_streams.properties(price, title, link, pictures, address, bedrooms, bathrooms, receptions, epc_rating, tenure, time_remaining_on_lease, service_charge, council_tax_band, ground_rent)

    #     VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    # """, kwargs.values())
    # print("Data inserted successfully!")
    # logging.info("Data inserted successfully!")

def create_cassandra_session():
    # Specify contact points and load balancing policy 
    # For Spark Job
    cluster = Cluster(contact_points=["172.20.0.4"], port=9042,
                      load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
                      protocol_version=4)
    session = cluster.connect()
    # Specify contact points and load balancing policy
    # cluster = Cluster(contact_points=["127.0.0.1"], port=9042,
    #                   load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
    #                   protocol_version=4)
    # session = cluster.connect()
    
    logging.info("Session created successfully-1")
    
    create_keyspace(session)  # Ensure this function is defined elsewhere
    session.set_keyspace('property_streams')
    
    logging.info("Session created successfully-2") 
    
    create_table(session)  # Ensure this function is defined elsewhere
    
    logging.info("Session created successfully-3")
    return session

def process_row(row):
    from cassandra.cluster import Cluster
    from cassandra.policies import DCAwareRoundRobinPolicy
    import logging
    logging.info("------------------------Processing Row for Cassandra-----------------------")
    # Specify contact points and load balancing policy 
    # For Spark Job
    cluster = Cluster(contact_points=["172.20.0.4"], port=9042,
                      load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
                      protocol_version=4,
                      connect_timeout=20)
    session = cluster.connect("property_streams")
    logging.info("--------------------------ABC---------------------------------")
    # Setup the Cassandra connection
    # cluster = Cluster(contact_points=["127.0.0.1"], port=9042,
    #                   load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
    #                   protocol_version=4)
    # session = cluster.connect('property_streams')
    
    # Prepare row data as a dictionary if coming from Spark DataFrame
    row_data = row.asDict() if hasattr(row, "asDict") else row
    logging.info('Preparing row data as a dictionary')
    # Log data to see what is being processed
    logging.info(f"Processing row data: {row_data}")

    # Execute your query using the existing function
    try:
        insert_data(session, **row_data)
        logging.info(f"Inserted row data: {row_data}")
    except Exception as e:
        logging.error(f"Failed to process row {row}: {str(e)}")
    finally:
        session.shutdown()
        cluster.shutdown()

def process_batch(batch_df, batch_id):
    # Process each row independently
    batch_df.foreach(process_row)


# def process_batch(batch_df, batch_id, cassandra_session):
#     logging.info("Processing batch" + str(batch_id))
#     try:
#         batch_df.foreach(lambda row: insert_data(cassandra_session, **row.asDict()))
#     except Exception as e:
#         logging.error(f"Failed to process batch {batch_id}: {str(e)}")
#     finally:
#         cassandra_session.shutdown()

# def process_batch(batch_df, batch_id):
#     # Create session inside the function
#     cassandra_session = create_cassandra_session()
#     try:
#         logging.info("Processing batch" + str(batch_id))
#         batch_df.foreach(lambda row: insert_data(cassandra_session, **row.asDict()))
#     except Exception as e:
#         logging.error(f"Failed to process batch {batch_id}: {str(e)}")
#     finally:
#         cassandra_session.shutdown()  # Close the session after the batch is processed

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info('-------------------------1--------------------------')
    # Initialize Spark session
    try:
        spark = (SparkSession.builder
                .appName("RealEstateConsumer")
                .config("spark.hadoop.native.lib", "false")
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                .config("spark.hadoop.native.lib", "C:\\hadoop\\hadoop-3.3.6\\lib\\native")
                .config("spark.cassandra.connection.host", "localhost")
                .config("spark.jars.packages",
                        "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1," 
                        #  "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview1"
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
                        )
                .getOrCreate())
        spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    except Exception as e:
        logging.error(f"Failed to initialize Spark session: {str(e)}")
        return
    logging.info('-------------------------2--------------------------')
    # Create Cassandra session once
    try:
        cassandra_session = create_cassandra_session()
    except Exception as e:
        logging.error(f"Failed to create Cassandra session: {str(e)}")
        return
    logging.info('-------------------------3--------------------------')
    # Read from Kafka
    try:
        kafka_df = (spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers", "ks-project-kafka-broker-1:29092")  # Ensure Kafka is running on the correct port
                    .option("subscribe", "properties")
                    .option("startingOffsets", "earliest")
                    .load())
    except Exception as e:
        logging.error(f"Failed to read from Kafka: {str(e)}")
        return
    logging.info('-------------------------4--------------------------')
    # Define schema for Kafka data
    schema = StructType([
        StructField("price", StringType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("link", StringType(), nullable=True),
        StructField("data_extracted", StringType(), nullable=True),
        StructField("pictures", ArrayType(StringType()), nullable=True),
        StructField("address", StringType(), nullable=True),
        StructField("bedrooms", StringType(), nullable=True),
        StructField("bathrooms", StringType(), nullable=True),
        StructField("receptions", StringType(), nullable=True),
        StructField("EPC Rating", StringType(), nullable=True),
        StructField("tenure", StringType(), nullable=True),
        StructField("time_remaining_on_lease", StringType(), nullable=True),
        StructField("service_charge", StringType(), nullable=True),
        StructField("council_tax_band", StringType(), nullable=True),
        StructField("ground_rent", StringType(), nullable=True)
    ])
    logging.info('-------------------------5--------------------------')

    # Process Kafka messages
    kafka_df = (kafka_df
                .selectExpr("CAST(value AS STRING) AS value")
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.*"))
    logging.info('-------------------------6--------------------------')
    # Insert data into Cassandra using foreachBatch
    try: 
        # cassandra_query = (kafka_df.writeStream
        #                 .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id, cassandra_session))
        #                 .start()
        #                 .awaitTermination())
        cassandra_query = kafka_df.writeStream.foreachBatch(process_batch).start().awaitTermination()
    except Exception as e:
        logging.error(f"Failed to process batch: {str(e)}")
        return
    logging.info('-------------------------7--------------------------')
if __name__ == "__main__":
    main()