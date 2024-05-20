from tecton import Entity

driver_entity = Entity(
    name='driver',
    description="Represents a driver in the transportation system",
    join_keys=['DRIVER_ID'],
    owner="jon@tecton.ai",
    tags={
        "environment": "development",
        "use_case": "driver_behavior_analysis",
        "team": "logistics"
    }  
)

requester_entity = Entity(
    name='requester',
    description="Represents an individual or company requesting transportation services",
    join_keys=['REQUESTER_ID'],
    owner="jon@tecton.ai",
    tags={
        "environment": "development",
        "use_case": "customer_activity_tracking",
        "team": "customer_relations"
    }  
)
