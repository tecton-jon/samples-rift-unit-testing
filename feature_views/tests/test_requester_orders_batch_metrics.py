# import pytest
# import pandas as pd
# from datetime import datetime
# from unittest.mock import patch
# import tecton
# import snowflake.connector
# import os

# from feature_views.requester_orders_batch_metrics import requester_orders_batch_metrics

# # Configuring Snowflake connection
# @pytest.fixture(scope="module")
# def configure_snowflake_session():
#     connection_parameters = {
#         "user": os.environ['SNOWFLAKE_USER'],
#         "password": os.environ['SNOWFLAKE_PASSWORD'],
#         "account": os.environ['SNOWFLAKE_ACCOUNT'],
#         "role": os.environ['SNOWFLAKE_ROLE'],
#         "warehouse": os.environ['SNOWFLAKE_WAREHOUSE'],
#         "database": os.environ['SNOWFLAKE_DATABASE'],
#         "schema": os.environ['SNOWFLAKE_SCHEMA']
#     }
#     conn = snowflake.connector.connect(**connection_parameters)
#     tecton.snowflake_context.set_connection(conn)
#     tecton.conf.set("TECTON_BATCH_COMPUTE_MODE", "rift")
#     tecton.conf.set("TECTON_OFFLINE_RETRIEVAL_COMPUTE_MODE", "rift")
#     return conn

# # Define the test
# def test_requester_orders_batch_metrics():
#     # Mock input data
#     input_df = pd.DataFrame(
#         {
#             "ORDER_CREATED": [datetime(2024, 5, 10), datetime(2024, 5, 11), datetime(2024, 5, 12)],
#             "ORDER_ID": ["ORD-001", "ORD-002", "ORD-003"],
#             "REQUESTER_ID": ["REQ-001", "REQ-001", "REQ-002"],
#             "PAYMENT_AMOUNT": [42.42, 55.42, 100.00],
#         }
#     )

#     events_df = pd.DataFrame(
#         {
#             "REQUESTER_ID": ["REQ-001", "REQ-001", "REQ-002"],
#             "ORDER_CREATED": [datetime(2024, 5, 12), datetime(2024, 5, 13), datetime(2024, 5, 14)],
#         }
#     )

#     tecton.conf.set("TECTON_BATCH_COMPUTE_MODE", "rift")
#     tecton.conf.set("TECTON_OFFLINE_RETRIEVAL_COMPUTE_MODE", "rift")
#     #requester_orders_batch_metrics.validate()

#     output_df = requester_orders_batch_metrics.get_features_for_events(
#             events_df, mock_inputs={"orders": input_df}, from_source=True
#     )

#     expected_df = pd.DataFrame({
#         'REQUESTER_ID': ['REQ-001', 'REQ-001', 'REQ-002'],
#         'ORDER_CREATED': ['2024-05-12', '2024-05-13', '2024-05-14'],
#         'requester_order_batch_metrics__ORDER_ID_count_30d_1d': [2, 2, 1],
#         'requester_order_batch_metrics__PAYMENT_AMOUNT_mean_30d_1d': [48.92, 48.92, 100.00]
#     })
#     expected_df

#     # NOTE: It is important to sort the dataframe to avoid test flakes.
#     output_df = output_df.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
#     expected_df = expected_df.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
    
#     pd.testing.assert_frame_equal(output_df, expected_df)

