from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from block_data import get_latest_transactions



# Get data of latest block
tx_data = get_latest_transactions()
if tx_data:
# Initialize PySpark session
    spark = SparkSession.builder.appName("Web3PySparkAnalysis").getOrCreate()

    # Define Schema for DataFrame
    schema = StructType([
        StructField("hash", StringType(), False),
        StructField("from", StringType(), False),
        StructField("to", StringType(), True),
       StructField("value", DecimalType(38,18), False),  # Large decimal type for ETH values
    StructField("gas", DecimalType(38,0), False),  # Large integer type for gas
    StructField("gasPrice", DecimalType(38,18), False)  # Large decimal type for gasPrice
    ])

    # Create PySpark DataFrame directly from list of dictionaries
    df = spark.createDataFrame(tx_data, schema=schema)

    # Count transactions per sender
    df_sender_count = df.groupBy("from").agg(count("hash").alias("transaction_count"))

    # Total ETH sent per sender
    df_sender_value = df.groupBy("from").agg(sum("value").alias("total_eth_sent"))

    # Average gas price per sender
    df_avg_gas_price = df.groupBy("from").agg(avg("gasPrice").alias("avg_gas_price_gwei"))

    # Maximum and minimum transaction values
    df_max_tx_value = df.agg(max("value").alias("max_transaction_value"))
    df_min_tx_value = df.agg(min("value").alias("min_transaction_value"))

    # Show results
    df_sender_count.show()
    df_sender_value.show()
    df_avg_gas_price.show()
    df_max_tx_value.show()
    df_min_tx_value.show()

    # Stop Spark session
    spark.stop()
else:
    print("No transactions retrieved. Skipping PySpark processing.")