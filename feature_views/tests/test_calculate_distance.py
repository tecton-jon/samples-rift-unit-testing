# import pandas as pd
# import pytest
# import tecton

# from feature_views.calculate_distance import calculate_distance

# # Sample data for testing
# @pytest.fixture
# def sample_orders_batch_metrics():
#     return {
#         'orders_stream_metrics': {
#             'PICKUP_LATITUDE_last_30d_continuous': 34.0522,
#             'PICKUP_LONGITUDE_last_30d_continuous': -118.2437,
#             'DROPOFF_LATITUDE_last_30d_continuous': 36.7783,
#             'DROPOFF_LONGITUDE_last_30d_continuous': -119.4179
#         }
#     }

# def test_calculate_distance(sample_orders_batch_metrics):
#     # Expected results
#     expected = {'dist_km': 348.35310218728745}

#     tecton.conf.set("TECTON_BATCH_COMPUTE_MODE", "rift")

#     # Invoke run_transformation function of the On-Demand Feature View
#     actual = calculate_distance.run_transformation(input_data=sample_orders_batch_metrics)

#     # Assert that the actual results match the expected results
#     pd.testing.assert_frame_equal(actual, expected)