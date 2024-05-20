from tecton import on_demand_feature_view
from tecton.types import Float64, Field, String

from feature_views.requester_orders_batch_metrics import requester_orders_batch_metrics


output_schema = [Field('dist_km', Float64), Field('error', String)]

@on_demand_feature_view(
    sources=[requester_orders_batch_metrics],
    mode='python',
    schema=output_schema,
    description="How far a transaction is from the user's home",
)
def calculate_distance(orders_batch_metrics):
    import math

    try:
        # Ensure latitude and longitude values are valid
        if None in (orders_batch_metrics['PICKUP_LATITUDE_last_30d_continuous'], orders_batch_metrics['PICKUP_LONGITUDE_last_30d_continuous'], 
                    orders_batch_metrics['DROPOFF_LATITUDE_last_30d_continuous'], orders_batch_metrics['DROPOFF_LONGITUDE_last_30d_continuous']):
            return {'dist_km': float(-1)}

        # Convert coordinates from degrees to radians and ensure they are floats
        pickup_lat = math.radians(float(orders_batch_metrics['PICKUP_LATITUDE_last_30d_continuous']))
        pickup_lon = math.radians(float(orders_batch_metrics['PICKUP_LONGITUDE_last_30d_continuous']))
        dropoff_lat = math.radians(float(orders_batch_metrics['DROPOFF_LATITUDE_last_30d_continuous']))
        dropoff_lon = math.radians(float(orders_batch_metrics['DROPOFF_LONGITUDE_last_30d_continuous']))

        # Haversine formula to calculate distance
        delta_lat = dropoff_lat - pickup_lat
        delta_lon = dropoff_lon - pickup_lon
        a = math.sin(delta_lat / 2.0)**2 + math.cos(pickup_lat) * math.cos(dropoff_lat) * math.sin(delta_lon / 2.0)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        radius_earth_km = 6371.0  # Radius of the Earth in kilometers, explicitly as a float
        distance = radius_earth_km * c

        return {
            'dist_km': distance,
            'error': None
        }
    except Exception as e:
        # Return -1 in case of any error encountered during calculation
        return {
            'dist_km': float(-1),
            'error': str(e)
        }
