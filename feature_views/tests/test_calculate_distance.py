import pandas as pd
import pytest
import tecton

from feature_views.calculate_distance import calculate_distance

# Sample data for testing
@pytest.fixture
def sample_orders_batch_metrics():
    return {
        'orders_batch_metrics': {
            'PICKUP_LATITUDE_last_30d_continuous': 34.0522,
            'PICKUP_LONGITUDE_last_30d_continuous': -118.2437,
            'DROPOFF_LATITUDE_last_30d_continuous': 36.7783,
            'DROPOFF_LONGITUDE_last_30d_continuous': -119.4179
        }
    }

def test_calculate_distance(sample_orders_batch_metrics):
    tecton.conf.set("TECTON_BATCH_COMPUTE_MODE", "rift")

    # Expected results
    expected = {'dist_km': 321.25354627586313, 'error': None}

    # Invoke run_transformation function of the On-Demand Feature View
    actual = calculate_distance.run_transformation(input_data=sample_orders_batch_metrics)

    # Assert that the actual results match the expected results
    assert actual == expected, "The actual output does not match the expected output."
