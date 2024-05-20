import os
from datetime import datetime
import pandas as pd
import pytest
import tecton
import snowflake.connector
from unittest.mock import patch
from feature_views.requester_orders_batch_metrics import requester_orders_batch_metrics

@pytest.fixture(scope="module", autouse=True)
def snowflake_session():
    """Fixture to configure the Snowflake session."""
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

def test_requester_orders_batch_metrics():
    input_df = pd.DataFrame({
        "ORDER_CREATED": [datetime(2024, 5, 10), datetime(2024, 5, 11), datetime(2024, 5, 12)],
        "ORDER_ID": ["ORD-001", "ORD-002", "ORD-003"],
        "REQUESTER_ID": ["REQ-001", "REQ-001", "REQ-002"],
        "PAYMENT_AMOUNT": [42.42, 55.42, 100.00]
    })

    events_df = pd.DataFrame({
        "REQUESTER_ID": ["REQ-001", "REQ-001", "REQ-002"],
        "ORDER_CREATED": [datetime(2024, 5, 12), datetime(2024, 5, 13), datetime(2024, 5, 14)],
    })

    output_df = requester_orders_batch_metrics.get_features_for_events(
        events_df, mock_inputs={"orders": input_df}, from_source=True, compute_mode="rift"
    )

    actual = output_df.to_pandas()

    expected = pd.DataFrame({
        'REQUESTER_ID': ['REQ-001', 'REQ-001', 'REQ-002'],
        'ORDER_CREATED': [datetime(2024, 5, 12), datetime(2024, 5, 13), datetime(2024, 5, 14)],
        'requester_orders_batch_metrics__ORDER_ID_count_30d_1d': [2, 2, 1],
        'requester_orders_batch_metrics__PAYMENT_AMOUNT_mean_30d_1d': [48.92, 48.92, 100.00]
    })

    # NOTE: It is important to sort the dataframe to avoid test flakes.
    actual = actual.sort_values(["REQUESTER_ID", "ORDER_CREATED"]).reset_index(drop=True)
    expected = expected.sort_values(["REQUESTER_ID", "ORDER_CREATED"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(actual, expected)
