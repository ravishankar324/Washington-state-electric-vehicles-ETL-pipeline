import sys
import pytest
import tempfile
import os
from pyspark.sql import SparkSession
from transform import create_vehicles_types_dim, create_location_dim, create_electric_utility_dim, join_and_create_fact_table, create_spark_session, read_data, write_data \

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()
    yield spark
    spark.stop()

def test_create_vehicles_types_dim(spark):
    input_data = [("Tesla", "Model S", 2020, "Electric", "Yes", 370)]
    df = spark.createDataFrame(input_data, ["make", "model", "model_year", "vehicle_type", "CAFV_eligibility", "electric_range"])

    df_vehiclestypes = create_vehicles_types_dim(df)
    
    expected_data = [("Tesla", "Model S", 2020, "Electric", "Yes", 370, 1)]
    expected_df = spark.createDataFrame(expected_data, ["make", "model", "model_year", "vehicle_type", "CAFV_eligibility", "electric_range", "vehicletype_id"])
    
    assert df_vehiclestypes.collect() == expected_df.collect()

def test_create_location_dim(spark):
    input_data = [("King", "Seattle", "WA", 98101)]
    df = spark.createDataFrame(input_data, ["county", "city", "state", "postal_code"])

    df_location = create_location_dim(df)
    
    expected_data = [("King", "Seattle", "WA", 98101, 1)]
    expected_df = spark.createDataFrame(expected_data, ["county", "city", "state", "postal_code", "location_id"])
    
    assert df_location.collect() == expected_df.collect()

def test_create_electric_utility_dim(spark):
    input_data = [("Seattle City Light",)]
    df = spark.createDataFrame(input_data, ["electric_utility"])

    df_electricutility = create_electric_utility_dim(df)
    
    expected_data = [("Seattle City Light", 1)]
    expected_df = spark.createDataFrame(expected_data, ["electric_utility", "electric_utility_id"])
    
    assert df_electricutility.collect() == expected_df.collect()

def test_join_and_create_fact_table(spark):
    df_1_data = [("Tesla", "Model S", 2020, "Electric", "Yes", 370, "VIN1234", "King", "Seattle", "WA", 98101, "Seattle City Light")]
    df_vehiclestypes_data = [("Tesla", "Model S", 2020, "Electric", "Yes", 370, 1)]
    df_location_data = [("King", "Seattle", "WA", 98101, 1)]
    df_electricutility_data = [("Seattle City Light", 1)]

    df_1 = spark.createDataFrame(df_1_data, ["make", "model", "model_year", "vehicle_type", "CAFV_eligibility", "electric_range", "vin_number","county", "city", "state", "postal_code", "electric_utility"])
    df_vehiclestypes = spark.createDataFrame(df_vehiclestypes_data, ["make", "model", "model_year", "vehicle_type", "CAFV_eligibility", "electric_range", "vehicletype_id"])
    df_location = spark.createDataFrame(df_location_data, ["county", "city", "state", "postal_code", "location_id"])
    df_electricutility = spark.createDataFrame(df_electricutility_data, ["electric_utility", "electric_utility_id"])

    df_fact = join_and_create_fact_table(df_1, df_vehiclestypes, df_location, df_electricutility)
    
    expected_data = [( "VIN1234", 1, 1, 1)]
    expected_df = spark.createDataFrame(expected_data, ["vin_number","vehicletype_id", "location_id", "electric_utility_id"])
    
    assert df_fact.collect() == expected_df.collect()

def test_write_data(spark):
    input_data = [("Tesla", "Model S", 2020, "Electric", "Yes", 370, 1)]
    df = spark.createDataFrame(input_data, ["make", "model", "model_year", "vehicle_type", "CAFV_eligibility", "electric_range", "vehicletype_id"])

    with tempfile.TemporaryDirectory() as tmpdirname:
        output_path = os.path.join(tmpdirname, "vehicletype_details")
        
        write_data(df, output_path)
        
        result_df = spark.read.parquet(output_path)
        
        expected_data = [("Tesla", "Model S", 2020, "Electric", "Yes", 370, 1)]
        expected_df = spark.createDataFrame(expected_data, ["make", "model", "model_year", "vehicle_type", "CAFV_eligibility", "electric_range", "vehicletype_id"])
        
        assert result_df.collect() == expected_df.collect()
