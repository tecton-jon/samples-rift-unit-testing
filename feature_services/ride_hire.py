from tecton import FeatureService

from feature_views.requester_orders_batch_metrics import requester_orders_batch_metrics
from feature_views.calculate_distance import calculate_distance

ride_hire = FeatureService(
    name="ride_hire",
    description="Features for ride_hire model",
    online_serving_enabled=True,
    features=[
        requester_orders_batch_metrics,
        calculate_distance
    ],
    tags={
        "use_case": "Ride Hire",
        "environment": "development"
    },
    owner="jon@tecton.ai",
)