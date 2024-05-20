import pandas as pd
import pytest
from datetime import datetime
import os
import tecton
import snowflake.connector
from data_sources.orders_snowflake_batch_source import orders_batch_source

@pytest.fixture(scope="module")
def snowflake_connection():
    connection_parameters = {
        "user": os.getenv('SNOWFLAKE_USER'),
        "password": os.getenv('SNOWFLAKE_PASSWORD'),
        "account": os.getenv('SNOWFLAKE_ACCOUNT'),
        "role": os.getenv('SNOWFLAKE_ROLE'),
        "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
        "database": os.getenv('SNOWFLAKE_DATABASE'),
        "schema": os.getenv('SNOWFLAKE_SCHEMA')
    }
    conn = snowflake.connector.connect(**connection_parameters)
    tecton.snowflake_context.set_connection(conn)
    tecton.set_validation_mode('auto')
    tecton.conf.set("TECTON_OFFLINE_RETRIEVAL_COMPUTE_MODE", "rift")
    yield conn
    conn.close()

@pytest.fixture
def sample_orders_data():
    return pd.DataFrame({
        'ORDER_ID': ['ORD000001', 'ORD000002', 'ORD000003'],
        'REQUESTER_ID': ['REQ00001', 'REQ00002', 'REQ00003'],
        'REQUESTER_RATING': [4.5, 4.3, 4.7],
        'DRIVER_ID': ['DRV00001', 'DRV00002', 'DRV00003'],
        'DRIVER_RATING': [4.8, 4.6, 4.9],
        'DRIVER_STATUS': ['available', 'busy', 'available'],
        'PICKUP_LATITUDE': [34.0522, 34.0523, 34.0524],
        'PICKUP_LONGITUDE': [-118.2437, -118.2440, -118.2441],
        'DROPOFF_LATITUDE': [36.7783, 36.7784, 36.7785],
        'DROPOFF_LONGITUDE': [-119.4179, -119.4180, -119.4181],
        'CARGO_SIZE': ['medium', 'small', 'large'],
        'PAYMENT_METHOD': ['credit_card', 'debit_card', 'paypal'],
        'PAYMENT_AMOUNT': [150.0, 180.0, 200.0],
        'ORDER_CREATED': [datetime(2024, 4, 1, 8, 0), datetime(2024, 4, 2, 9, 0), datetime(2024, 4, 3, 10, 0)],
        'VEHICLE_TYPE': ['closed_truck', 'open_truck', 'van']
    })

@pytest.mark.usefixtures("snowflake_connection")
def test_get_dataframe(sample_orders_data):
    start = datetime(2024, 4, 1, 8)
    end = datetime(2024, 4, 3, 10, 0, 1)
    tecton.conf.set("TECTON_BATCH_COMPUTE_MODE", "rift")
    orders_batch_source.validate()
    df = orders_batch_source.get_dataframe(start_time=start, end_time=end, compute_mode="rift").to_pandas()
    pd.testing.assert_frame_equal(df, sample_orders_data)
