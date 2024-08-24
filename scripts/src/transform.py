import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col, lit, row_number, broadcast
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Creating Spark session
def create_spark_session():
    logger.info("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("ETL_script") \
        .getOrCreate()
    logger.info("Spark session created successfully.")
    return spark

# Reading CSV file
def read_data(spark, input_path, schema):
    logger.info(f"Reading data from {input_path}...")
    df = spark.read.options(header='True').schema(schema).csv(input_path)
    logger.info(f"Data read successfully with {df.count()} records.")
    return df

# Creating vehicletype dim
def create_vehicles_types_dim(df):
    logger.info("Creating vehicle types dimension table...")
    df_2 = df.select("make", "model", "model_year", "vehicle_type", "CAFV_eligibility", "electric_range").distinct()
    df_vehiclestypes = df_2.withColumn("vehicletype_id", row_number().over(Window.orderBy("make")))
    logger.info("Vehicle types dimension table created.")
    return df_vehiclestypes

# Creating location dim
def create_location_dim(df):
    logger.info("Creating location dimension table...")
    df_3 = df.select("county", "city", "state", "postal_code").distinct()
    df_location = df_3.withColumn("location_id", row_number().over(Window.orderBy("county")))
    logger.info("Location dimension table created.")
    return df_location

# Creating electric utility dim
def create_electric_utility_dim(df):
    logger.info("Creating electric utility dimension table...")
    df_4 = df.select("electric_utility").distinct()
    df_electricutility = df_4.withColumn("electric_utility_id", row_number().over(Window.orderBy("electric_utility")))
    logger.info("Electric utility dimension table created.")
    return df_electricutility

# Creating vehicles fact table
def join_and_create_fact_table(df_1, df_vehiclestypes, df_location, df_electricutility):
    logger.info("Joining dimension tables to create fact table...")
    df_join = df_1.join(broadcast(df_vehiclestypes),
                        (df_1["make"] == df_vehiclestypes["make"]) &
                        (df_1["model"] == df_vehiclestypes["model"]) &
                        (df_1["model_year"] == df_vehiclestypes["model_year"]) &
                        (df_1["vehicle_type"] == df_vehiclestypes["vehicle_type"]) &
                        (df_1["electric_range"] == df_vehiclestypes["electric_range"]) &
                        (df_1["CAFV_eligibility"] == df_vehiclestypes["CAFV_eligibility"]),
                        "inner")
    df_join = df_join.drop("make", "model", "model_year", "vehicle_type", "CAFV_eligibility", "electric_range")

    df_join = df_join.join(broadcast(df_location),
                           (df_join["county"] == df_location["county"]) &
                           (df_join["city"] == df_location["city"]) &
                           (df_join["state"] == df_location["state"]) &
                           (df_join["postal_code"] == df_location["postal_code"]), "inner")
    df_join = df_join.drop("county", "city", "state", "postal_code")

    df_join = df_join.join(broadcast(df_electricutility),
                           (df_join["electric_utility"] == df_electricutility["electric_utility"]), "inner")
    df_join = df_join.drop("electric_utility")
    
    logger.info("Fact table created successfully.")
    return df_join

# Writing transformed dataframes to S3 
def write_data(df, output_path):
    logger.info(f"Writing data to {output_path}...")
    df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Data written successfully to {output_path}.")

# Calling transformations functions
def transform(input_path):
    # Creating Spark session 
    spark = create_spark_session()

    # Creating schema for data
    schema = StructType([StructField("vin_number", StringType(), True),
                         StructField("county", StringType(), True),
                         StructField("city", StringType(), True),
                         StructField("state", StringType(), True),
                         StructField("postal_code", IntegerType(), True),
                         StructField("model_year", IntegerType(), True),
                         StructField("make", StringType(), True),
                         StructField("model", StringType(), True),
                         StructField("vehicle_type", StringType(), True),
                         StructField("CAFV_eligibility", StringType(), True),
                         StructField("electric_range", IntegerType(), True),
                         StructField("base_msrp", IntegerType(), True),
                         StructField("Legislative_district", StringType(), True),
                         StructField("vehicle_id", StringType(), True),
                         StructField("vehicle_location", StringType(), True),
                         StructField("electric_utility", StringType(), True),
                         StructField("2020_census_tract", LongType(), True)])

    logger.info("Starting data transformation process...")
    
    df = read_data(spark, input_path, schema)
    df.cache()  # Caching initial read df as it will be used multiple times

    # Adding primary Key to initial df
    df_1 = df.withColumn("vehicle_id", row_number().over(Window.orderBy("vin_number")))

    # Creating vehicles dim table from original df and writing to S3
    df_vehiclestypes = create_vehicles_types_dim(df)
    write_data(df_vehiclestypes, "s3a://washigton-electricvehicles-data-analysis-spark-etl/output/vehicletype_details/")

    # Creating location dims table from original df and writing to S3
    df_location = create_location_dim(df)
    write_data(df_location, "s3a://washigton-electricvehicles-data-analysis-spark-etl/output/location_details/")

    # Creating electric utility dims table from original df and writing to S3 
    df_electricutility = create_electric_utility_dim(df)
    write_data(df_electricutility, "s3a://washigton-electricvehicles-data-analysis-spark-etl/output/electricutility_details/")

    # Joining dims tables to original table to create reference and write fact table to S3
    df_join = join_and_create_fact_table(df_1, df_vehiclestypes, df_location, df_electricutility)
    write_data(df_join, "s3a://washigton-electricvehicles-data-analysis-spark-etl/output/vehicle_details/")

    # Uncache the cached df to release data from memory
    df.unpersist()

    # Stop the Spark session
    spark.stop()
    logger.info("Transformation process completed successfully.")

if __name__ == '__main__':
    # Reading input S3 location and calling transform function
    input_dir = "s3a://washigton-electricvehicles-data-analysis-spark-etl/input/Electric_Vehicle_Population_Data.csv"
    transform(input_dir)
