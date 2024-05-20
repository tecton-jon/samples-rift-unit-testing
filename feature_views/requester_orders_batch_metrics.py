from datetime import datetime, timedelta

from tecton import batch_feature_view, Aggregation, RiftBatchConfig
from tecton.types import Field, String, Timestamp, Float64

from entities import requester_entity
from data_sources.orders_snowflake_batch_source import orders_batch_source

@batch_feature_view(
    sources=[orders_batch_source],
    entities=[requester_entity],
    mode="snowflake_sql",
    aggregations=[
        Aggregation(column="ORDER_ID", function="count", time_window=timedelta(days=30)),
        Aggregation(column="PAYMENT_AMOUNT", function="mean", time_window=timedelta(days=30)),
    ],
    aggregation_interval=timedelta(days=1),
    schema=[Field('ORDER_CREATED', Timestamp),
            Field('ORDER_ID', String),
            Field('REQUESTER_ID', String),
            # Field("REQUESTER_RATING", dtype=Float64),
            # Field("DRIVER_ID", dtype=String),
            # Field("DRIVER_RATING", dtype=Float64),
            # Field("DRIVER_STATUS", dtype=String),
            # Field("PICKUP_LATITUDE", dtype=Float64),
            # Field("PICKUP_LONGITUDE", dtype=Float64),
            # Field("DROPOFF_LATITUDE", dtype=Float64),
            # Field("DROPOFF_LONGITUDE", dtype=Float64),
            # Field("CARGO_SIZE", dtype=String),
            # Field("PAYMENT_METHOD", dtype=String),
            Field("PAYMENT_AMOUNT", dtype=Float64),
            # Field("VEHICLE_TYPE", dtype=String)
    ],
    description="Calculates the number of orders placed by each requester over the last 7 and 30 days",
    online=True,
    offline=True,
    feature_start_time=datetime(2024, 1, 1),
    batch_compute=RiftBatchConfig(),
    tags={
        "use_case": "Ride Hire",
        "environment": "development"
    },
    owner="jon@tecton.ai",
    run_transformation_validation=False
)
def requester_orders_batch_metrics(orders):
    return f'''
        SELECT 
            ORDER_CREATED,
            ORDER_ID,
            REQUESTER_ID,
            PAYMENT_AMOUNT
        FROM {orders}
        '''
