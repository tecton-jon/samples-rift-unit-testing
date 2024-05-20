from tecton import BatchSource, SnowflakeConfig, Secret

# SQL query to select data from the orders table - alternatively, we can just configure a table to query.
orders_snowflake_config = SnowflakeConfig(
    url='https://tectonpartner.snowflakecomputing.com',
    database="NAB",
    schema="PUBLIC",
    warehouse="NAB_WH",
    #user=Secret(scope='snowflake-scope', key='snowflake-user'),
    #password=Secret(scope='snowflake-scope', key='snowflake-password'),
    query=('''
        SELECT 
            order_id,
            requester_id,
            requester_rating::DOUBLE AS requester_rating,
            driver_id,
            driver_rating::DOUBLE AS driver_rating,
            driver_status,
            pickup_latitude::DOUBLE AS pickup_latitude,
            pickup_longitude::DOUBLE AS pickup_longitude,
            dropoff_latitude::DOUBLE AS dropoff_latitude,
            dropoff_longitude::DOUBLE AS dropoff_longitude,
            cargo_size,
            payment_method,
            payment_amount::DOUBLE AS payment_amount,
            order_created,
            vehicle_type
        FROM 
            public.orders
    '''),
    timestamp_field="ORDER_CREATED"
)

# Configure BatchSource with the Snowflake Config
orders_batch_source = BatchSource(
    name="orders_snowflake_batch_source",
    description="Returns detailed order transactions data.",
    batch_config=orders_snowflake_config,
    tags={
        "use_case": "Ride Hire",
        "environment": "development"
    },
    owner="jon@tecton.ai",
)